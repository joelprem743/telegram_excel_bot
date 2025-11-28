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
S_WAITING_PICK = "WAITING_PICK"   # user picks one of candidate full-values
S_WAITING_CONFIRM = "WAITING_CONFIRM"

# in-memory session store: chat_id -> dict
# For production consider persistent storage (redis) if you need reliability across restarts.
sessions = {}

app = Flask(__name__)

TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    logger.error("TOKEN environment variable not set. Set TOKEN in Render environment.")
    raise SystemExit("TOKEN not set")

SERVICE_NAME = os.environ.get("RENDER_SERVICE_NAME", "telegram_excel_bot")
# webhook path will be /webhook/<TOKEN>
WEBHOOK_PATH = f"/webhook/{TOKEN}"
WEBHOOK_URL = f"https://{SERVICE_NAME}.onrender.com{WEBHOOK_PATH}"

bot = Bot(token=TOKEN)


# -------------------------
# Utilities
# -------------------------
def stringify_cell(val):
    """Return a cleaned string representation for cells:
       - dates => DD-MM-YYYY
       - floats that are ints => as int without scientific notation
       - other values => str trimmed
    """
    if val is None:
        return ""
    # Excel stores dates as datetime objects in openpyxl
    if isinstance(val, (datetime, date)):
        return val.strftime("%d-%m-%Y")
    # xlrd may return float for dates if not parsed; we don't try to detect excel date floats here
    # If it's a float that is a whole number, convert to int to avoid scientific notation
    if isinstance(val, float):
        # big integer-like floats should be represented without decimals
        if val.is_integer():
            return str(int(val))
        else:
            # format with no scientific notation, up to 12 decimals trimmed
            return format(val, "f").rstrip("0").rstrip(".")
    # If it's numeric (int)
    if isinstance(val, int):
        return str(val)
    # Otherwise string-like
    s = str(val).strip()
    return s


def load_excel_clean(file_bytes: bytes, file_name: str):
    """
    Returns a tuple (rows_list, headers_list)
    - rows_list: list of rows, each row is list of cleaned values (stringified)
    - headers_list: list of header column values (string)
    Supports .xlsx (openpyxl), .xls (xlrd), and ".xls" that are actually xlsx (caught).
    """
    ext = file_name.lower()
    rows = []
    headers = []

    if ext.endswith(".xlsx"):
        wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
        ws = wb.active
        # iterate rows values_only to get native Python values, then stringify
        rows_iter = list(ws.iter_rows(values_only=True))
        if not rows_iter:
            return [], []
        headers = [stringify_cell(v) or f"Column {i+1}" for i, v in enumerate(rows_iter[0])]
        for r in rows_iter[1:]:
            rows.append([stringify_cell(v) for v in r])
        return rows, headers

    elif ext.endswith(".xls"):
        try:
            book = xlrd.open_workbook(file_contents=file_bytes)
            sheet = book.sheet_by_index(0)
            if sheet.nrows == 0:
                return [], []
            # first row = headers
            headers = [stringify_cell(sheet.cell_value(0, c)) or f"Column {c+1}" for c in range(sheet.ncols)]
            for r in range(1, sheet.nrows):
                row_vals = [stringify_cell(sheet.cell_value(r, c)) for c in range(sheet.ncols)]
                rows.append(row_vals)
            return rows, headers
        except xlrd.biffh.XLRDError as e:
            # handle disguised xlsx saved with .xls extension
            msg = str(e).lower()
            if "xlsx file; not supported" in msg or "expected xls workbook" in msg:
                # try openpyxl
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

    else:
        raise ValueError("Unsupported file type. Use .xls or .xlsx")


def build_output_workbook(header_row, rows_to_write):
    """Return BytesIO containing an .xlsx workbook with header_row and rows_to_write"""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Filtered"
    ws.append(header_row)
    for row in rows_to_write:
        # ensure row length matches header length by padding with empty strings
        if len(row) < len(header_row):
            row = row + [""] * (len(header_row) - len(row))
        ws.append(row)
    # Basic column width adjust
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
# Bot interaction helpers
# -------------------------
def init_session(chat_id):
    sessions[chat_id] = {
        "state": S_WAITING_FILE,
        "file_name": None,
        "headers": None,
        "rows": None,         # list of rows (each row is list of strings)
        "max_col": 0,
        "chosen_col": None,
        "candidates": None,   # list of distinct matching full-values for substring
    }
    return sessions[chat_id]


def get_session(chat_id):
    return sessions.get(chat_id) or init_session(chat_id)


def cleanup_session(chat_id):
    if chat_id in sessions:
        del sessions[chat_id]


# -------------------------
# Handlers (message processing)
# -------------------------
def handle_start(chat_id):
    init_session(chat_id)
    text = (
        "Send an Excel file (.xls or .xlsx). I will strip formatting and process raw values.\n\n"
        "If your file is .xls but actually an xlsx, the loader handles that automatically."
    )
    bot.send_message(chat_id=chat_id, text=text)


def handle_document(chat_id, file_id, file_name):
    sess = get_session(chat_id)
    try:
        logger.info("Downloading file %s for chat %s", file_name, chat_id)
        f = bot.get_file(file_id)
        data = f.download_as_bytearray()
        rows, headers = load_excel_clean(bytes(data), file_name)

        if not headers:
            bot.send_message(chat_id=chat_id, text="Could not find headers or the file is empty. Send another file.")
            return

        sess["file_name"] = file_name
        sess["headers"] = headers
        sess["rows"] = rows
        sess["max_col"] = len(headers)
        sess["state"] = S_WAITING_COLUMN

        # present headers with numbers
        lines = [f"{i+1}. {h}" for i, h in enumerate(headers)]
        bot.send_message(
            chat_id=chat_id,
            text=f"File loaded. Found {len(headers)} columns:\n\n" + "\n".join(lines) + "\n\nSend column number to filter by."
        )
    except Exception as e:
        logger.exception("Error reading file")
        bot.send_message(chat_id=chat_id, text=f"Error reading file: {e}")


def handle_text(chat_id, text):
    sess = get_session(chat_id)
    state = sess["state"]

    if text.strip().startswith("/start"):
        handle_start(chat_id)
        return

    # CANCEL
    if text.strip().lower() in ("/cancel", "cancel", "stop"):
        cleanup_session(chat_id)
        bot.send_message(chat_id=chat_id, text="Operation cancelled. Send /start to begin again.")
        return

    # State machine
    if state == S_WAITING_FILE:
        bot.send_message(chat_id=chat_id, text="Please send an Excel file (.xls or .xlsx) first.")
        return

    if state == S_WAITING_COLUMN:
        # Expect numeric column index
        try:
            col = int(text.strip())
            if not (1 <= col <= sess["max_col"]):
                bot.send_message(chat_id=chat_id, text=f"Invalid column number. Choose 1..{sess['max_col']}")
                return
            sess["chosen_col"] = col - 1  # store 0-based index
            sess["state"] = S_WAITING_SUBSTRING
            bot.send_message(chat_id=chat_id, text=f"Column {col} selected ({sess['headers'][col-1]}). Now send a substring to search for (loose match).")
            return
        except ValueError:
            bot.send_message(chat_id=chat_id, text="Please send a valid column number.")
            return

    if state == S_WAITING_SUBSTRING:
        # user sent substring to search: find distinct values in chosen column that contain substring (case-insensitive)
        substr = text.strip().lower()
        col_idx = sess["chosen_col"]
        values = []
        for row in sess["rows"]:
            # protect for rows shorter than expected
            v = row[col_idx] if col_idx < len(row) else ""
            s = (v or "").strip()
            if substr in s.lower():
                values.append(s)
        distinct = []
        seen = set()
        for v in values:
            if v not in seen:
                seen.add(v)
                distinct.append(v)
        if not distinct:
            bot.send_message(chat_id=chat_id, text=f"No column values contain '{text}'. Send another substring or /cancel.")
            return
        # If only one match, proceed directly to filter
        if len(distinct) == 1:
            chosen_full = distinct[0]
            bot.send_message(chat_id=chat_id, text=f"Found single match: {chosen_full}. Filtering rows now...")
            do_filter_and_send(chat_id, sess, chosen_full)
            cleanup_session(chat_id)
            return
        # Many candidates: present numbered list for user to pick
        sess["candidates"] = distinct
        sess["state"] = S_WAITING_PICK
        lines = [f"{i+1}. {v}" for i, v in enumerate(distinct)]
        bot.send_message(chat_id=chat_id, text="Found multiple matches:\n" + "\n".join(lines) + "\n\nSend the number of the correct value.")
        return

    if state == S_WAITING_PICK:
        # Expect user to pick number of candidate
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

    # fallback
    bot.send_message(chat_id=chat_id, text="I didn't understand that. Send /start to begin.")


def do_filter_and_send(chat_id, sess, chosen_full_value):
    """Filter sess['rows'] where chosen_col equals chosen_full_value (string compare) and send an xlsx to user."""
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

        # Build workbook and send
        bio = build_output_workbook(header, filtered)
        filename_base = os.path.splitext(sess.get("file_name", "output"))[0]
        out_name = f"{filename_base}_filtered.xlsx"

        # Telegram InputFile
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
    # Security: verify token in path; Telegram posts here.
    try:
        if request.headers.get("content-type", "").startswith("application/json"):
            data = request.get_json(force=True)
        else:
            data = request.get_json(force=True)
    except Exception:
        logger.exception("Invalid request to webhook")
        abort(400)

    try:
        update = Update.de_json(data, bot)
    except Exception:
        # not a Telegram update
        logger.exception("Failed parsing Update")
        return "ok"

    # handle message updates only
    if update.message:
        chat_id = update.effective_chat.id
        # ensure session exists
        sess = get_session(chat_id)
        # handle document
        if update.message.document:
            file_id = update.message.document.file_id
            file_name = update.message.document.file_name or "file"
            handle_document(chat_id, file_id, file_name)
            return "ok"
        # handle text
        if update.message.text:
            text = update.message.text
            handle_text(chat_id, text)
            return "ok"
        # file types we don't support
        bot.send_message(chat_id=chat_id, text="Unsupported message type. Send an Excel document or /start.")
        return "ok"

    # callback_query, edited_message, etc are ignored for now
    return "ok"


# -------------------------
# Start & webhook registration
# -------------------------
def set_webhook():
    try:
        # set webhook to the /webhook/<TOKEN> path
        logger.info("Setting webhook to %s", WEBHOOK_URL)
        ok = bot.set_webhook(WEBHOOK_URL)
        logger.info("Webhook set: %s", ok)
    except Exception:
        logger.exception("Failed to set webhook")


if __name__ == "__main__":
    # register webhook on start
    set_webhook()
    port = int(os.environ.get("PORT", 10000))
    # render provides $PORT for web service; listening on 0.0.0.0 required
    logger.info("Starting Flask on port %s", port)
    app.run(host="0.0.0.0", port=port)
