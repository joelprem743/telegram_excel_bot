# bot.py
import os
import logging
import traceback
from io import BytesIO
from datetime import datetime, date

from flask import Flask, request, abort
from telegram import Bot, Update, InputFile
import openpyxl
import xlrd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# States
S_WAITING_FILE = "WAITING_FILE"
S_WAITING_COLUMN = "WAITING_COLUMN"
S_WAITING_SUBSTRING = "WAITING_SUBSTRING"
S_WAITING_PICK = "WAITING_PICK"

# in-memory session store: chat_id -> dict
sessions = {}

app = Flask(__name__)

TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    logger.error("TOKEN environment variable not set. Set TOKEN in Render environment.")
    raise SystemExit("TOKEN not set")

# Use Render provided external hostname when possible (recommended)
RENDER_HOSTNAME = os.environ.get("RENDER_EXTERNAL_HOSTNAME")
SERVICE_NAME = os.environ.get("RENDER_SERVICE_NAME") or os.environ.get("RENDER_SERVICE_ID") or "telegram_excel_bot"
if RENDER_HOSTNAME:
    HOSTNAME = RENDER_HOSTNAME
else:
    # fallback to SERVICE_NAME + .onrender.com (works for many dev setups)
    HOSTNAME = f"{SERVICE_NAME}.onrender.com"

WEBHOOK_PATH = f"/webhook/{TOKEN}"
WEBHOOK_URL = f"https://{HOSTNAME}{WEBHOOK_PATH}"

bot = Bot(token=TOKEN)


# -------------------------
# Utilities
# -------------------------
def stringify_cell(val):
    """Return a cleaned string representation for cells:
       - dates => DD-MM-YYYY
       - floats that are integers => no decimal, avoid scientific notation
       - other numbers => plain string without scientific notation
       - None => empty string
    """
    if val is None:
        return ""
    # datetime/date
    if isinstance(val, (datetime, date)):
        return val.strftime("%d-%m-%Y")
    # openpyxl may give numbers as floats
    if isinstance(val, float):
        if val.is_integer():
            # large floats may exceed exact integer precision; this is best-effort
            try:
                return str(int(val))
            except OverflowError:
                return format(val, "f").rstrip("0").rstrip(".")
        return format(val, "f").rstrip("0").rstrip(".")
    if isinstance(val, int):
        return str(val)
    # For other objects
    s = str(val).strip()
    return s


def load_excel_clean(file_bytes: bytes, file_name: str):
    """
    Returns (rows_list, headers_list)
    - rows_list: list of rows, each row is list of cleaned values (string)
    - headers_list: list of header column values (string)
    Supports .xlsx, .xls and .xls disguised xlsx.
    """
    ext = file_name.lower()
    rows = []
    headers = []

    # .xlsx
    if ext.endswith(".xlsx"):
        wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True, read_only=False)
        ws = wb.active
        rows_iter = list(ws.iter_rows(values_only=True))
        if not rows_iter:
            return [], []
        headers = [stringify_cell(v) or f"Column {i+1}" for i, v in enumerate(rows_iter[0])]
        for r in rows_iter[1:]:
            # ensure full row by padding
            row_vals = [stringify_cell(v) for v in r]
            rows.append(row_vals)
        return rows, headers

    # .xls (real or disguised)
    if ext.endswith(".xls"):
        try:
            book = xlrd.open_workbook(file_contents=file_bytes)
            sheet = book.sheet_by_index(0)
            if sheet.nrows == 0:
                return [], []
            # headers
            headers = [stringify_cell(sheet.cell_value(0, c)) or f"Column {c+1}" for c in range(sheet.ncols)]
            for r in range(1, sheet.nrows):
                row_vals = [stringify_cell(sheet.cell_value(r, c)) for c in range(sheet.ncols)]
                rows.append(row_vals)
            return rows, headers
        except xlrd.biffh.XLRDError as e:
            # some .xls are actually .xlsx; detect common message and retry with openpyxl
            msg = str(e).lower()
            if "xlsx file; not supported" in msg or "expected xls workbook" in msg or "file is not a spreadsheet" in msg:
                try:
                    wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True, read_only=False)
                    ws = wb.active
                    rows_iter = list(ws.iter_rows(values_only=True))
                    if not rows_iter:
                        return [], []
                    headers = [stringify_cell(v) or f"Column {i+1}" for i, v in enumerate(rows_iter[0])]
                    for r in rows_iter[1:]:
                        rows.append([stringify_cell(v) for v in r])
                    return rows, headers
                except Exception:
                    # fallthrough to raise original
                    logger.exception("Failed to load disguised xlsx")
                    raise
            raise

    raise ValueError("Unsupported file type. Use .xls or .xlsx")


def build_output_workbook(header_row, rows_to_write):
    """Return BytesIO containing an .xlsx workbook with header_row and rows_to_write"""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Filtered"
    ws.append(header_row)
    for row in rows_to_write:
        # pad row to header length
        if len(row) < len(header_row):
            row = row + [""] * (len(header_row) - len(row))
        ws.append(row)
    # adjust widths
    for col_idx in range(1, ws.max_column + 1):
        max_len = 0
        for r in range(1, ws.max_row + 1):
            cell = ws.cell(r, col_idx).value
            if cell is None:
                continue
            l = len(str(cell))
            if l > max_len:
                max_len = l
        width = min(max(8, max_len + 2), 50)
        ws.column_dimensions[openpyxl.utils.get_column_letter(col_idx)].width = width
    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio


# -------------------------
# Session helpers
# -------------------------
def init_session(chat_id):
    sessions[chat_id] = {
        "state": S_WAITING_FILE,
        "file_name": None,
        "headers": None,
        "rows": None,
        "max_col": 0,
        "chosen_col": None,
        "candidates": None,
    }
    return sessions[chat_id]


def get_session(chat_id):
    return sessions.get(chat_id) or init_session(chat_id)


def cleanup_session(chat_id):
    if chat_id in sessions:
        del sessions[chat_id]


# -------------------------
# Handlers
# -------------------------
def handle_start(chat_id):
    init_session(chat_id)
    text = (
        "Send an Excel file (.xls or .xlsx). I will strip formatting and process raw values.\n\n"
        "Flow:\n"
        "1) Upload file\n"
        "2) Send column number to search\n"
        "3) Send partial substring (e.g. 'avanigadda')\n"
        "4) I'll list matching distinct values to choose from\n"
        "5) I'll send an .xlsx containing only the rows that match the chosen value\n\n"
        "Send /cancel at any time."
    )
    bot.send_message(chat_id=chat_id, text=text)


def handle_document(chat_id, file_id, file_name):
    sess = get_session(chat_id)
    try:
        logger.info("Downloading file %s for chat %s", file_name, chat_id)
        tf = bot.get_file(file_id)
        # warn if huge? we just download; if you expect very large files add size checks here.
        data = tf.download_as_bytearray()
        rows, headers = load_excel_clean(bytes(data), file_name)

        if not headers:
            bot.send_message(chat_id=chat_id, text="Could not find headers or the file is empty. Send another file.")
            return

        sess["file_name"] = file_name
        sess["headers"] = headers
        sess["rows"] = rows
        sess["max_col"] = len(headers)
        sess["state"] = S_WAITING_COLUMN

        lines = [f"{i+1}. {h}" for i, h in enumerate(headers)]
        bot.send_message(
            chat_id=chat_id,
            text=f"File loaded. Found {len(headers)} columns:\n\n" + "\n".join(lines) + "\n\nSend column number to filter by."
        )
    except Exception as e:
        logger.exception("Error reading file for chat %s", chat_id)
        bot.send_message(chat_id=chat_id, text=f"Error reading file: {e}\nSend another file or /cancel.")


def handle_text(chat_id, text):
    sess = get_session(chat_id)
    state = sess["state"]

    if text.strip().startswith("/start"):
        handle_start(chat_id)
        return

    # Cancel
    if text.strip().lower() in ("/cancel", "cancel", "stop"):
        cleanup_session(chat_id)
        bot.send_message(chat_id=chat_id, text="Operation cancelled. Send /start to begin again.")
        return

    if state == S_WAITING_FILE:
        bot.send_message(chat_id=chat_id, text="Please send an Excel file (.xls or .xlsx) first.")
        return

    if state == S_WAITING_COLUMN:
        try:
            col = int(text.strip())
            if not (1 <= col <= sess["max_col"]):
                bot.send_message(chat_id=chat_id, text=f"Invalid column number. Choose 1..{sess['max_col']}")
                return
            sess["chosen_col"] = col - 1
            sess["state"] = S_WAITING_SUBSTRING
            bot.send_message(chat_id=chat_id, text=f"Column {col} selected ({sess['headers'][col-1]}). Now send a substring to search for (case-insensitive).")
            return
        except ValueError:
            bot.send_message(chat_id=chat_id, text="Please send a valid column number (e.g. 3).")
            return

    if state == S_WAITING_SUBSTRING:
        substr = text.strip().lower()
        col_idx = sess["chosen_col"]
        values = []
        for row in sess["rows"]:
            v = row[col_idx] if col_idx < len(row) else ""
            s = (v or "").strip()
            if substr in s.lower():
                values.append(s)
        # distinct preserving order
        distinct = []
        seen = set()
        for v in values:
            if v not in seen:
                seen.add(v)
                distinct.append(v)
        if not distinct:
            bot.send_message(chat_id=chat_id, text=f"No column values contain '{text}'. Send another substring or /cancel.")
            return
        if len(distinct) == 1:
            chosen_full = distinct[0]
            bot.send_message(chat_id=chat_id, text=f"Found single match: {chosen_full}. Filtering rows now...")
            do_filter_and_send(chat_id, sess, chosen_full)
            cleanup_session(chat_id)
            return
        sess["candidates"] = distinct
        sess["state"] = S_WAITING_PICK
        # limit list length to safe size (e.g. 200) to avoid spamming
        max_show = 200
        enumerated = distinct[:max_show]
        lines = [f"{i+1}. {v}" for i, v in enumerate(enumerated)]
        extra = "" if len(distinct) <= max_show else f"\n...and {len(distinct)-max_show} more results (be more specific)"
        bot.send_message(chat_id=chat_id, text="Found multiple matches:\n" + "\n".join(lines) + extra + "\n\nSend the number of the correct value.")
        return

    if state == S_WAITING_PICK:
        try:
            n = int(text.strip())
            cand = sess.get("candidates") or []
            if not (1 <= n <= len(cand)):
                bot.send_message(chat_id=chat_id, text=f"Invalid selection. Send a number between 1 and {len(cand)}.")
                return
            chosen_full = cand[n-1]
            bot.send_message(chat_id=chat_id, text=f"You selected: {chosen_full}. Filtering rows now...")
            do_filter_and_send(chat_id, sess, chosen_full)
            cleanup_session(chat_id)
            return
        except ValueError:
            bot.send_message(chat_id=chat_id, text="Send the number of the desired value (e.g. 2).")
            return

    bot.send_message(chat_id=chat_id, text="I didn't understand that. Send /start to begin.")


def do_filter_and_send(chat_id, sess, chosen_full_value):
    try:
        col_idx = sess["chosen_col"]
        header = sess["headers"]
        rows = sess["rows"]

        filtered = []
        for r in rows:
            v = r[col_idx] if col_idx < len(r) else ""
            if (v or "").strip().lower() == chosen_full_value.strip().lower():
                filtered.append(r)

        if not filtered:
            bot.send_message(chat_id=chat_id, text=f"No rows found for value '{chosen_full_value}'.")
            return

        bio = build_output_workbook(header, filtered)
        filename_base = os.path.splitext(sess.get("file_name", "output"))[0]
        out_name = f"{filename_base}_filtered.xlsx"

        bio.seek(0)
        bot.send_document(chat_id=chat_id, document=InputFile(bio, filename=out_name),
                          caption=f"Filtered results: {len(filtered)} rows for '{chosen_full_value}'.")
    except Exception:
        logger.exception("Error while filtering/sending file")
        bot.send_message(chat_id=chat_id, text="Error while filtering or sending file.")


# -------------------------
# Flask webhook endpoint
# -------------------------
@app.route(WEBHOOK_PATH, methods=["POST"])
def webhook():
    # Only accept JSON
    if not request.headers.get("content-type", "").startswith("application/json"):
        abort(400)
    try:
        data = request.get_json(force=True)
    except Exception:
        logger.exception("Invalid request body")
        abort(400)

    try:
        update = Update.de_json(data, bot)
    except Exception:
        logger.exception("Failed parsing Update json")
        return "ok"

    # handle message updates only
    if update.message:
        chat_id = update.effective_chat.id
        # create session if not exist
        get_session(chat_id)
        # document
        if update.message.document:
            file_id = update.message.document.file_id
            file_name = update.message.document.file_name or "file"
            handle_document(chat_id, file_id, file_name)
            return "ok"
        # text
        if update.message.text:
            text = update.message.text
            handle_text(chat_id, text)
            return "ok"
        # other message types
        bot.send_message(chat_id=chat_id, text="Unsupported message type. Send an Excel document or /start.")
        return "ok"

    # ignore other update types for now
    return "ok"


# -------------------------
# Webhook registration
# -------------------------
def set_webhook():
    try:
        logger.info("Deleting existing webhook (if any)")
        bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        logger.exception("Failed to delete existing webhook (continuing)")

    try:
        logger.info("Setting webhook to %s", WEBHOOK_URL)
        ok = bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set: %s", ok)
    except Exception:
        logger.exception("Failed to set webhook. Make sure your public URL is reachable and TOKEN is correct.")


if __name__ == "__main__":
    # register webhook on start
    set_webhook()
    port = int(os.environ.get("PORT", 10000))
    logger.info("Starting Flask on port %s (webhook path: %s)", port, WEBHOOK_PATH)
    # For production with Render use gunicorn: e.g. "gunicorn bot:app"
    app.run(host="0.0.0.0", port=port)
