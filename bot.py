import os
import logging
from io import BytesIO
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, ConversationHandler, filters
)
import openpyxl
import xlrd
from rapidfuzz import process, fuzz

load_dotenv()

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

WAIT_FILE, WAIT_COLUMN, WAIT_RAWVALUE, WAIT_CHOICE = range(4)

TOKEN = os.getenv("TOKEN")
if not TOKEN:
    raise RuntimeError("TOKEN not found in environment variables")

application = Application.builder().token(TOKEN).build()


# ----------------------------------------------------------------------------------------------------
# Load XLS/XLSX safely
# ----------------------------------------------------------------------------------------------------
def load_excel_clean(file_bytes: bytes, file_name: str):
    ext = file_name.lower()

    # XLSX
    if ext.endswith(".xlsx"):
        wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
        ws = wb.active

        clean = openpyxl.Workbook()
        dst = clean.active

        for row in ws.iter_rows(values_only=True):
            dst.append(list(row))

        return clean

    # XLS (true or fake)
    if ext.endswith(".xls"):
        try:
            book = xlrd.open_workbook(file_contents=file_bytes)
            sheet = book.sheet_by_index(0)

            clean = openpyxl.Workbook()
            dst = clean.active

            for r in range(sheet.nrows):
                dst.append(sheet.row_values(r))

            return clean

        except xlrd.biffh.XLRDError as e:
            if "xlsx file; not supported" in str(e).lower():
                wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
                ws = wb.active

                clean = openpyxl.Workbook()
                dst = clean.active

                for row in ws.iter_rows(values_only=True):
                    dst.append(list(row))

                return clean
            raise e

    raise ValueError("Unsupported file type")


# ----------------------------------------------------------------------------------------------------
# Conversation: START
# ----------------------------------------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Send an Excel file (.xls or .xlsx).")
    return WAIT_FILE


# ----------------------------------------------------------------------------------------------------
# Receive file
# ----------------------------------------------------------------------------------------------------
async def receive_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        doc = update.message.document
        name = doc.file_name.lower()

        if not (name.endswith(".xls") or name.endswith(".xlsx")):
            await update.message.reply_text("Only .xls or .xlsx allowed.")
            return WAIT_FILE

        tgfile = await context.bot.get_file(doc.file_id)
        data = await tgfile.download_as_bytearray()

        wb = load_excel_clean(bytes(data), name)
        ws = wb.active

        headers = []
        for c in range(1, ws.max_column + 1):
            headers.append(f"{c}. {ws.cell(1, c).value}")

        context.user_data["wb"] = wb
        context.user_data["name"] = name
        context.user_data["cols"] = ws.max_column

        await update.message.reply_text(
            "File loaded.\n\nColumns:\n" +
            "\n".join(headers) +
            "\n\nSend column number to filter."
        )
        return WAIT_COLUMN

    except Exception as e:
        logger.error(e)
        await update.message.reply_text("Error reading file.")
        return WAIT_FILE


# ----------------------------------------------------------------------------------------------------
# Receive column number
# ----------------------------------------------------------------------------------------------------
async def receive_column(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        col = int(update.message.text.strip())
        if not 1 <= col <= context.user_data["cols"]:
            await update.message.reply_text("Invalid column number.")
            return WAIT_COLUMN

        context.user_data["col"] = col
        await update.message.reply_text("Enter search text (partial allowed).")
        return WAIT_RAWVALUE

    except:
        await update.message.reply_text("Enter a valid number.")
        return WAIT_COLUMN


# ----------------------------------------------------------------------------------------------------
# Fuzzy match unique values
# ----------------------------------------------------------------------------------------------------
async def receive_raw_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.message.text.strip().lower()
    col = context.user_data["col"]

    ws = context.user_data["wb"].active

    values = set()
    for r in range(2, ws.max_row + 1):
        v = ws.cell(r, col).value
        if v:
            values.add(str(v).strip())

    values = list(values)

    matches = process.extract(
        raw,
        values,
        scorer=fuzz.WRatio,
        limit=8
    )

    good = [m for m in matches if m[1] >= 60]
    if not good:
        await update.message.reply_text("No close matches found.")
        return ConversationHandler.END

    formatted = []
    for idx, (val, score, _) in enumerate(good, start=1):
        formatted.append(f"{idx}. {val}")

    context.user_data["choices"] = [v[0] for v in good]

    await update.message.reply_text(
        "Found multiple matches:\n" +
        "\n".join(formatted) +
        "\n\nSend the number of your correct value."
    )
    return WAIT_CHOICE


# ----------------------------------------------------------------------------------------------------
# User selects final exact match → filter rows
# ----------------------------------------------------------------------------------------------------
async def receive_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        idx = int(update.message.text.strip())
        choices = context.user_data["choices"]

        if not 1 <= idx <= len(choices):
            await update.message.reply_text("Invalid choice.")
            return WAIT_CHOICE

        final = choices[idx - 1].lower()
        col = context.user_data["col"]
        ws = context.user_data["wb"].active

        out = openpyxl.Workbook()
        dst = out.active

        header = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]
        dst.append(header)

        count = 0
        for r in range(2, ws.max_row + 1):
            cell = ws.cell(r, col).value
            if cell and str(cell).strip().lower() == final:
                row = [ws.cell(r, c).value for c in range(1, ws.max_column + 1)]
                dst.append(row)
                count += 1

        if count == 0:
            await update.message.reply_text("No rows found.")
            return ConversationHandler.END

        bio = BytesIO()
        out.save(bio)
        bio.seek(0)

        base = os.path.splitext(context.user_data["name"])[0]
        fname = f"{base}_filtered.xlsx"

        await update.message.reply_document(
            document=bio,
            filename=fname,
            caption=f"Done. {count} rows."
        )

        return ConversationHandler.END

    except Exception as e:
        logger.error(e)
        await update.message.reply_text("Error.")
        return ConversationHandler.END


# ----------------------------------------------------------------------------------------------------
# Cancel handler
# ----------------------------------------------------------------------------------------------------
async def cancel(update, context):
    await update.message.reply_text("Canceled.")
    return ConversationHandler.END


# ----------------------------------------------------------------------------------------------------
# Add handlers
# ----------------------------------------------------------------------------------------------------
conv = ConversationHandler(
    entry_points=[CommandHandler("start", start)],
    states={
        WAIT_FILE: [MessageHandler(filters.Document.ALL, receive_file)],
        WAIT_COLUMN: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_column)],
        WAIT_RAWVALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_raw_value)],
        WAIT_CHOICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_choice)],
    },
    fallbacks=[CommandHandler("cancel", cancel)],
)

application.add_handler(conv)
