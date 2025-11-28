# bot.py
import os
import logging
from io import BytesIO
from datetime import datetime
from typing import List, Any
from dotenv import load_dotenv

from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters,
    ContextTypes, ConversationHandler
)

import openpyxl
import xlrd
from rapidfuzz import process, fuzz

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

WAITING_FILE, WAITING_COLUMN, WAITING_QUERY, WAITING_SELECT = range(4)

# ================================ UTILITIES ================================

def is_integer_like_number(v: Any) -> bool:
    try:
        if isinstance(v, float):
            return v.is_integer()
        if isinstance(v, int):
            return True
    except:
        return False
    return False

def parse_possible_date(value: Any):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        if isinstance(value, (int, float)):
            if 1000 < value < 100000:
                try:
                    dt = xlrd.xldate.xldate_as_datetime(value, 0)
                    return dt
                except:
                    pass
    except:
        pass
    if isinstance(value, str):
        s = value.strip()
        fmts = [
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
            "%d-%m-%Y", "%d/%m/%Y",
            "%Y/%m/%d", "%d-%b-%Y",
            "%d %b %Y", "%m/%d/%Y",
            "%Y.%m.%d %H:%M:%S", "%Y.%m.%d"
        ]
        for f in fmts:
            try:
                return datetime.strptime(s, f)
            except:
                continue
        try:
            if "t" in s.lower():
                return datetime.fromisoformat(s)
        except:
            pass
    return None

def format_cell_for_output(value: Any):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%d-%m-%Y")
    if isinstance(value, (int, float)) and is_integer_like_number(value):
        return str(int(value))
    d = parse_possible_date(value)
    if d:
        return d.strftime("%d-%m-%Y")
    if isinstance(value, str):
        return value.strip()
    return str(value)

# ================================ EXCEL LOADER ================================

def load_excel_clean(file_bytes: bytes, file_name: str) -> openpyxl.Workbook:
    lower = file_name.lower()

    if lower.endswith(".xlsx"):
        wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
        ws = wb.active
        clean = openpyxl.Workbook()
        ws2 = clean.active
        for row in ws.iter_rows(values_only=True):
            ws2.append(list(row))
        return clean

    if lower.endswith(".xls"):
        try:
            book = xlrd.open_workbook(file_contents=file_bytes)
            sheet = book.sheet_by_index(0)
            clean = openpyxl.Workbook()
            ws2 = clean.active
            for r in range(sheet.nrows):
                ws2.append(sheet.row_values(r))
            return clean
        except xlrd.biffh.XLRDError as e:
            if "xlsx" in str(e).lower():
                wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
                ws = wb.active
                clean = openpyxl.Workbook()
                ws2 = clean.active
                for row in ws.iter_rows(values_only=True):
                    ws2.append(list(row))
                return clean
            raise

    raise ValueError("Unsupported file type. Send .xls or .xlsx")

# ================================ BOT HANDLERS ================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Send an Excel file (.xls or .xlsx) to begin.\n"
        "I will clean the file → show columns → filter records → return filtered Excel."
    )
    return WAITING_FILE

async def receive_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        doc = update.message.document
        if not doc:
            await update.message.reply_text("Send .xls or .xlsx file.")
            return WAITING_FILE

        fname = doc.file_name.lower()
        if not(fname.endswith(".xls") or fname.endswith(".xlsx")):
            await update.message.reply_text("Only .xls/.xlsx supported.")
            return WAITING_FILE

        tgfile = await context.bot.get_file(doc.file_id)
        fb = await tgfile.download_as_bytearray()

        wb = load_excel_clean(bytes(fb), doc.file_name)
        ws = wb.active

        context.user_data["wb"] = wb
        context.user_data["file_name"] = doc.file_name
        context.user_data["max_col"] = ws.max_column

        headers = []
        for c in range(1, ws.max_column + 1):
            v = ws.cell(1, c).value
            headers.append(f"{c}. {v if v else 'Column ' + openpyxl.utils.get_column_letter(c)}")

        await update.message.reply_text(
            "File loaded.\nSelect column:\n\n" +
            "\n".join(headers) +
            f"\n\nEnter column number (1-{ws.max_column})."
        )
        return WAITING_COLUMN

    except Exception as e:
        logger.exception("File read error")
        await update.message.reply_text(f"Error: {e}")
        return WAITING_FILE

async def receive_column(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip()
    try:
        col = int(txt)
    except:
        await update.message.reply_text("Invalid column number.")
        return WAITING_COLUMN

    if col < 1 or col > context.user_data["max_col"]:
        await update.message.reply_text("Column out of range.")
        return WAITING_COLUMN

    context.user_data["col"] = col
    await update.message.reply_text("Enter search text (partial match).")
    return WAITING_QUERY

async def receive_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.message.text.strip()
    if not q:
        await update.message.reply_text("Enter non-empty search.")
        return WAITING_QUERY

    wb = context.user_data["wb"]
    ws = wb.active
    col = context.user_data["col"]

    seen = {}
    for r in range(2, ws.max_row + 1):
        raw = ws.cell(r, col).value
        if raw:
            norm = str(raw).lower().strip()
            if q.lower() in norm and norm not in seen:
                seen[norm] = str(raw)

    if not seen:
        await update.message.reply_text("No matches found.")
        return ConversationHandler.END

    ranked = process.extract(q, list(seen.values()), scorer=fuzz.partial_ratio, limit=40)
    candidates = [t[0] for t in ranked]

    context.user_data["candidates"] = candidates

    msg = ["Select value:"]
    for i, v in enumerate(candidates, 1):
        msg.append(f"{i}. {v}")
    msg.append("\nSend number (0 cancel).")

    await update.message.reply_text("\n".join(msg))
    return WAITING_SELECT

async def receive_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip()
    if not txt.isdigit():
        await update.message.reply_text("Enter number.")
        return WAITING_SELECT

    idx = int(txt)
    if idx == 0:
        await update.message.reply_text("Cancelled.")
        return ConversationHandler.END

    candidates = context.user_data["candidates"]
    if idx < 1 or idx > len(candidates):
        await update.message.reply_text("Invalid choice.")
        return WAITING_SELECT

    chosen = candidates[idx - 1]
    wb = context.user_data["wb"]
    ws = wb.active
    col = context.user_data["col"]

    out = openpyxl.Workbook()
    out_ws = out.active

    # header
    header = [str(ws.cell(1, c).value or "") for c in range(1, ws.max_column + 1)]
    out_ws.append(header)

    match_count = 0
    for r in range(2, ws.max_row + 1):
        val = ws.cell(r, col).value
        if val and chosen.lower() in str(val).lower():
            row = [format_cell_for_output(ws.cell(r, c).value)
                   for c in range(1, ws.max_column + 1)]
            out_ws.append(row)
            match_count += 1

    buf = BytesIO()
    out.save(buf)
    buf.seek(0)

    await update.message.reply_document(
        buf,
        filename="filtered.xlsx",
        caption=f"{match_count} rows matched."
    )
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END

# ================================ HEALTH ENDPOINT ================================
async def health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("OK")

# ================================ MAIN (RENDER-WEBHOOK) ================================

def main():
    load_dotenv()
    TOKEN = os.getenv("TOKEN")
    RENDER_URL = os.getenv("RENDER_URL")
    PORT = int(os.getenv("PORT", 10000))

    if not TOKEN:
        raise SystemExit("TOKEN missing.")
    if not RENDER_URL:
        raise SystemExit("RENDER_URL missing.")

    application = Application.builder().token(TOKEN).build()

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            MessageHandler(filters.Document.ALL, receive_file)
        ],
        states={
            WAITING_FILE: [MessageHandler(filters.Document.ALL, receive_file)],
            WAITING_COLUMN: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_column)],
            WAITING_QUERY: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_query)],
            WAITING_SELECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_select)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True
    )

    application.add_handler(conv)

    application.add_handler(CommandHandler("health", health))

    # START WEBHOOK (mandatory for Render free tier)
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=f"{TOKEN}",
        webhook_url=f"{RENDER_URL}/{TOKEN}"
    )


if __name__ == "__main__":
    main()
