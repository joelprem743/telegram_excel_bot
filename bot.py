# app.py
import os
import logging
from io import BytesIO
from datetime import datetime, date

from flask import Flask, request, abort
from telegram import Bot, Update, InputFile
from telegram.ext import Dispatcher, CommandHandler, MessageHandler, Filters
import openpyxl
import xlrd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# -------------------------
# Config / webhook URL
# -------------------------
TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    logger.error("TOKEN environment variable not set. Set TOKEN in Render environment.")
    raise SystemExit("TOKEN not set")

# Use Render-provided URL if available; otherwise default to the domain you provided
SERVICE_URL = os.environ.get("RENDER_EXTERNAL_URL") or "https://telegram-excel-bot-11.onrender.com"

WEBHOOK_PATH = f"/webhook/{TOKEN}"
WEBHOOK_URL = f"{SERVICE_URL.rstrip('/')}{WEBHOOK_PATH}"

# Flask app
app = Flask(__name__)

# Telegram Bot + Dispatcher (synchronous v13)
bot = Bot(token=TOKEN)
dispatcher = Dispatcher(bot, None, workers=0, use_context=True)

# -------------------------
# Simple in-memory sessions
# (chat_id -> dict). For production use Redis/persistent store.
# -------------------------
S_WAITING_FILE = "WAITING_FILE"
S_WAITING_COLUMN = "WAITING_COLUMN"
S_WAITING_SUBSTRING = "WAITING_SUBSTRING"
S_WAITING_PICK = "WAITING_PICK"

sessions = {}

def init_session(chat_id):
    sessions[chat_id] = {
        "state": S_WAITING_FILE,
        "file_name": None,
        "headers": None,
        "rows": None,       # list[list[str]]
        "chosen_col": None, # 0-based
        "candidates": None, # list[str]
    }
    return sessions[chat_id]

def get_session(chat_id):
    return sessions.get(chat_id) or init_session(chat_id)

def cleanup_session(chat_id):
    sessions.pop(chat_id, None)

# -------------------------
# Excel utilities
# -------------------------
def stringify_cell(val):
    """Normalized string output for different Excel cell types."""
    if val is None:
        return ""
    if isinstance(val, (datetime, date)):
        return val.strftime("%d-%m-%Y")
    # openpyxl sometimes returns floats for numbers; avoid scientific notation
    if isinstance(val, float):
        if val.is_integer():
            return str(int(val))
        return format(val, "f").rstrip("0").rstrip(".")
    if isinstance(val, int):
        return str(val)
    return str(val).strip()

def load_excel_clean(file_bytes: bytes, file_name: str):
    """Return (rows_list, headers_list)
       rows_list: list of rows (each row is list of stringified values) excluding header
       headers_list: list of header strings
    """
    ext = file_name.lower()
    rows = []
    headers = []

    if ext.endswith(".xlsx"):
        wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
        ws = wb.active
        rows_iter = list(ws.iter_rows(values_only=True))
        if not rows_iter:
            return [], []
        headers = [stringify_cell(v) or f"Column {i+1}" for i, v in enumerate(rows_iter[0])]
        for r in rows_iter[1:]:
            rows.append([stringify_cell(v) for v in r])
        return rows, headers

    if ext.endswith(".xls"):
        try:
            book = xlrd.open_workbook(file_contents=file_bytes)
            sheet = book.sheet_by_index(0)
            if sheet.nrows == 0:
                return [], []
            headers = [stringify_cell(sheet.cell_value(0, c)) or f"Column {c+1}" for c in range(sheet.ncols)]
            for r in range(1, sheet.nrows):
                row_vals = [stringify_cell(sheet.cell_value(r, c)) for c in range(sheet.ncols)]
                rows.append(row_vals)
            return rows, headers
        except xlrd.biffh.XLRDError as e:
            msg = str(e).lower()
            # Handle disguised xlsx saved as .xls
            if "xlsx file; not supported" in msg or "expected xls workbook" in msg:
                wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
                ws = wb.active
                rows_iter = list(ws.iter_rows(values_only=True))
                if not rows_iter:
                    return [], []
                headers = [stringify_cell(v) or f"Column {i+1}" for i, v in enumerate(rows_iter[0])]
                for r in rows_iter[1:]:
                    rows.append([stringify_cell(v) for v in r])
                return rows, headers
            raise

    raise ValueError("Unsupported file type. Use .xls or .xlsx")

def build_output_workbook(header_row, rows_to_write):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Filtered"
    ws.append(header_row)
    for row in rows_to_write:
        if len(row) < len(header_row):
            row = row + [""] * (len(header_row) - len(row))
        ws.append(row)
    # adjust widths
    for col_idx in range(1, ws.max_column + 1):
        max_len = 0
        for r in range(1, ws.max_row + 1):
            val = ws.cell(r, col_idx).value
            if val is None:
                continue
            l = len(str(val))
            if l > max_len:
                max_len = l
        ws.column_dimensions[openpyxl.utils.get_column_letter(col_idx)].width = min(max(8, max_len + 2), 50)
    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio

# -------------------------
# Handlers (these are added to Dispatcher)
# We'll implement similarly to the state machine used previously.
# -------------------------
def start(update, context):
    chat_id = update.effective_chat.id
    init_session(chat_id)
    context.bot.send_message(chat_id=chat_id,
                             text="Send an Excel file (.xls or .xlsx). I will strip formatting and process raw values.")

def document_handler(update, context):
    chat_id = update.effective_chat.id
    sess = get_session(chat_id)
    doc = update.message.document
    file_id = doc.file_id
    file_name = doc.file_name or "file.xlsx"
    try:
        # download file into memory
        tgfile = context.bot.get_file(file_id)
        bio = BytesIO()
        tgfile.download(out=bio)
        bio.seek(0)
        rows, headers = load_excel_clean(bio.read(), file_name)
        if not headers:
            context.bot.send_message(chat_id=chat_id, text="Could not read headers or file is empty. Send another file.")
            return
        sess["rows"] = rows
        sess["headers"] = headers
        sess["file_name"] = file_name
        sess["state"] = S_WAITING_COLUMN
        # present headers
        lines = [f"{i+1}. {h}" for i, h in enumerate(headers)]
        context.bot.send_message(chat_id=chat_id,
                                 text=f"File loaded. Found {len(headers)} columns:\n\n" + "\n".join(lines) +
                                      "\n\nSend column number to filter by.")
    except Exception as e:
        logger.exception("Error reading document")
        context.bot.send_message(chat_id=chat_id, text=f"Error reading file: {e}")

def text_handler(update, context):
    chat_id = update.effective_chat.id
    text = update.message.text.strip()
    sess = get_session(chat_id)

    # commands
    if text.startswith("/start"):
        start(update, context)
        return
    if text.lower() in ("/cancel", "cancel", "stop"):
        cleanup_session(chat_id)
        context.bot.send_message(chat_id=chat_id, text="Operation cancelled. Send /start to begin again.")
        return

    state = sess.get("state", S_WAITING_FILE)

    if state == S_WAITING_FILE:
        context.bot.send_message(chat_id=chat_id, text="Please send an Excel file (.xls or .xlsx) first.")
        return

    if state == S_WAITING_COLUMN:
        # expect column number
        try:
            col_num = int(text)
            if not (1 <= col_num <= len(sess["headers"])):
                context.bot.send_message(chat_id=chat_id, text=f"Invalid column number. Choose 1..{len(sess['headers'])}")
                return
            sess["chosen_col"] = col_num - 1
            sess["state"] = S_WAITING_SUBSTRING
            context.bot.send_message(chat_id=chat_id, text=f"Column {col_num} selected ({sess['headers'][col_num-1]}). Now send a substring to search for.")
            return
        except ValueError:
            context.bot.send_message(chat_id=chat_id, text="Send a valid column number (e.g. 3).")
            return

    if state == S_WAITING_SUBSTRING:
        substr = text.lower()
        col_idx = sess["chosen_col"]
        matches = []
        for r in sess["rows"]:
            v = r[col_idx] if col_idx < len(r) else ""
            s = (v or "").strip()
            if substr in s.lower():
                matches.append(s)
        # distinct preserving order
        distinct = []
        seen = set()
        for v in matches:
            if v not in seen:
                seen.add(v)
                distinct.append(v)
        if not distinct:
            context.bot.send_message(chat_id=chat_id, text=f"No column values contain '{text}'. Send another substring or /cancel.")
            return
        if len(distinct) == 1:
            chosen = distinct[0]
            context.bot.send_message(chat_id=chat_id, text=f"Found single match: {chosen}. Filtering rows now...")
            do_filter_and_send(context.bot, chat_id, sess, chosen)
            cleanup_session(chat_id)
            return
        # multiple candidates
        sess["candidates"] = distinct
        sess["state"] = S_WAITING_PICK
        lines = [f"{i+1}. {v}" for i, v in enumerate(distinct)]
        context.bot.send_message(chat_id=chat_id, text="Found multiple matches:\n" + "\n".join(lines) + "\n\nSend the number of the correct value.")
        return

    if state == S_WAITING_PICK:
        try:
            idx = int(text) - 1
            cand = sess.get("candidates") or []
            if not (0 <= idx < len(cand)):
                context.bot.send_message(chat_id=chat_id, text=f"Invalid selection. Send a number between 1 and {len(cand)}.")
                return
            chosen = cand[idx]
            context.bot.send_message(chat_id=chat_id, text=f"You selected: {chosen}. Filtering rows now...")
            do_filter_and_send(context.bot, chat_id, sess, chosen)
            cleanup_session(chat_id)
            return
        except ValueError:
            context.bot.send_message(chat_id=chat_id, text="Send the number of the desired value (e.g. 2).")
            return

    # fallback
    context.bot.send_message(chat_id=chat_id, text="I didn't understand that. Send /start to begin.")

def do_filter_and_send(bot_obj, chat_id, sess, chosen_value):
    col_idx = sess["chosen_col"]
    header = sess["headers"]
    rows = sess["rows"]
    filtered = []
    for r in rows:
        v = r[col_idx] if col_idx < len(r) else ""
        if (v or "").strip().lower() == chosen_value.strip().lower():
            filtered.append(r)
    if not filtered:
        bot_obj.send_message(chat_id=chat_id, text=f"No rows found for value '{chosen_value}'.")
        return
    bio = build_output_workbook(header, filtered)
    filename_base = os.path.splitext(sess.get("file_name", "filtered"))[0]
    out_name = f"{filename_base}_filtered.xlsx"
    bio.seek(0)
    bot_obj.send_document(chat_id=chat_id, document=InputFile(bio, filename=out_name),
                          caption=f"Filtered results: {len(filtered)} rows for '{chosen_value}'.")

# Register handlers with dispatcher
dispatcher.add_handler(CommandHandler("start", start))
dispatcher.add_handler(MessageHandler(Filters.document, document_handler))
dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, text_handler))

# -------------------------
# Flask webhook endpoint
# -------------------------
@app.route(WEBHOOK_PATH, methods=["POST"])
def webhook_route():
    # Telegram will POST JSON updates to this endpoint
    try:
        data = request.get_json(force=True)
    except Exception:
        logger.exception("Invalid webhook request")
        abort(400)

    try:
        update = Update.de_json(data, bot)
    except Exception:
        logger.exception("Failed to parse Update")
        return "ok"

    # Let the dispatcher process the update synchronously
    dispatcher.process_update(update)
    return "OK"

# -------------------------
# Register webhook on start
# -------------------------
def set_webhook():
    try:
        logger.info("Setting Telegram webhook to %s", WEBHOOK_URL)
        # PTB v13 Bot.set_webhook is synchronous
        ok = bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set: %s", ok)
    except Exception:
        logger.exception("Failed to set webhook")

if __name__ == "__main__":
    # set webhook and run Flask
    set_webhook()
    port = int(os.environ.get("PORT", 10000))
    logger.info("Starting Flask on port %s", port)
    # Use 0.0.0.0 so Render can route traffic
    app.run(host="0.0.0.0", port=port)
