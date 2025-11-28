import os
import logging
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

sessions = {}
app = Flask(__name__)

TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise SystemExit("TOKEN environment variable not set")

SERVICE_URL = os.environ.get("RENDER_EXTERNAL_URL")
if not SERVICE_URL:
    SERVICE_NAME = os.environ.get("RENDER_SERVICE_NAME", "telegram_excel_bot")
    SERVICE_URL = f"https://{SERVICE_NAME}.onrender.com"

WEBHOOK_PATH = f"/webhook/{TOKEN}"
WEBHOOK_URL = f"{SERVICE_URL}{WEBHOOK_PATH}"

bot = Bot(token=TOKEN)

def stringify(val):
    if val is None:
        return ""
    if isinstance(val, (datetime, date)):
        return val.strftime("%d-%m-%Y")
    if isinstance(val, float) and val.is_integer():
        return str(int(val))
    return str(val).strip()

def load_excel_clean(file_bytes, file_name):
    ext = file_name.lower()

    if ext.endswith(".xlsx"):
        wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        headers = [stringify(v) or f"Column {i+1}" for i, v in enumerate(rows[0])]
        cleaned = [[stringify(c) for c in r] for r in rows[1:]]
        return cleaned, headers

    if ext.endswith(".xls"):
        try:
            book = xlrd.open_workbook(file_contents=file_bytes)
            sheet = book.sheet_by_index(0)
            headers = [stringify(sheet.cell_value(0, c)) for c in range(sheet.ncols)]
            rows = []
            for r in range(1, sheet.nrows):
                rows.append([stringify(sheet.cell_value(r, c)) for c in range(sheet.ncols)])
            return rows, headers
        except xlrd.biffh.XLRDError:
            wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
            ws = wb.active
            rows = list(ws.iter_rows(values_only=True))
            headers = [stringify(v) for v in rows[0]]
            cleaned = [[stringify(c) for c in r] for r in rows[1:]]
            return cleaned, headers

    raise ValueError("Unsupported file type")

def new_session(chat):
    sessions[chat] = {
        "state": S_WAITING_FILE,
        "headers": None,
        "rows": None,
        "file_name": None,
        "col": None,
        "candidates": None,
    }

def send_start(chat):
    new_session(chat)
    bot.send_message(chat, "Send an Excel file (.xls or .xlsx).")

def handle_document(chat, file_id, file_name):
    f = bot.get_file(file_id)
    data = f.download_as_bytearray()
    rows, headers = load_excel_clean(bytes(data), file_name)

    sess = sessions[chat]
    sess["rows"] = rows
    sess["headers"] = headers
    sess["file_name"] = file_name
    sess["state"] = S_WAITING_COLUMN

    text = "Columns:\n" + "\n".join([f"{i+1}. {h}" for i,h in enumerate(headers)])
    bot.send_message(chat, text + "\nSend column number.")

def filter_and_send(chat, sess, value):
    col = sess["col"]
    rows = sess["rows"]
    headers = sess["headers"]

    filtered = [r for r in rows if r[col].lower() == value.lower()]
    if not filtered:
        bot.send_message(chat, "No rows found.")
        return

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(headers)
    for r in filtered:
        ws.append(r)

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)

    bot.send_document(
        chat,
        InputFile(bio, filename="filtered.xlsx"),
        caption=f"{len(filtered)} rows found"
    )

    del sessions[chat]

def handle_text(chat, text):
    if text.startswith("/start"):
        send_start(chat)
        return

    sess = sessions.get(chat)
    if not sess:
        send_start(chat)
        return

    state = sess["state"]

    if state == S_WAITING_FILE:
        bot.send_message(chat, "Send an Excel file first.")
        return

    if state == S_WAITING_COLUMN:
        if not text.isdigit():
            bot.send_message(chat, "Send a number.")
            return
        col = int(text)-1
        sess["col"] = col
        sess["state"] = S_WAITING_SUBSTRING
        bot.send_message(chat, "Send a substring to search.")
        return

    if state == S_WAITING_SUBSTRING:
        substr = text.lower()
        col = sess["col"]

        values = set()
        for r in sess["rows"]:
            v = r[col]
            if substr in v.lower():
                values.add(v)

        values = list(values)
        if not values:
            bot.send_message(chat, "No matches.")
            return

        if len(values) == 1:
            filter_and_send(chat, sess, values[0])
            return

        sess["candidates"] = values
        sess["state"] = S_WAITING_PICK
        bot.send_message(
            chat,
            "Found multiple:\n" +
            "\n".join([f"{i+1}. {v}" for i,v in enumerate(values)]) +
            "\nSend number."
        )
        return

    if state == S_WAITING_PICK:
        if not text.isdigit():
            bot.send_message(chat, "Send a number.")
            return
        i = int(text)-1
        values = sess["candidates"]
        if i < 0 or i >= len(values):
            bot.send_message(chat, "Invalid choice.")
            return
        filter_and_send(chat, sess, values[i])
        return

@app.post(WEBHOOK_PATH)
def webhook():
    data = request.get_json()
    update = Update.de_json(data, bot)

    if update.message:
        chat = update.message.chat.id
        if update.message.document:
            handle_document(chat, update.message.document.file_id,
                            update.message.document.file_name)
        elif update.message.text:
            handle_text(chat, update.message.text)

    return "OK"

# register webhook
bot.set_webhook(WEBHOOK_URL)
print("Webhook set:", WEBHOOK_URL)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
