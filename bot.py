import os
import logging
from io import BytesIO
from rapidfuzz import process, fuzz
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler,
)
import openpyxl
import xlrd
from datetime import datetime

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

WAITING_FILE, WAITING_COLUMN, WAITING_SEARCH, WAITING_VALUE_SELECT, WAITING_FILTER = range(5)


# ==================================================================
# CLEAN LOADER (XLS + XLSX + ERROR-FIXING)
# ==================================================================
def load_excel_clean(file_bytes, file_name):
    ext = file_name.lower()

    # XLSX
    if ext.endswith(".xlsx"):
        wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
        ws = wb.active

        clean_wb = openpyxl.Workbook()
        clean_ws = clean_wb.active

        for row in ws.iter_rows(values_only=True):
            clean_ws.append(list(_clean_row(row)))

        return clean_wb

    # XLS
    if ext.endswith(".xls"):
        try:
            book = xlrd.open_workbook(file_contents=file_bytes)
            sheet = book.sheet_by_index(0)

            clean_wb = openpyxl.Workbook()
            clean_ws = clean_wb.active

            for r in range(sheet.nrows):
                clean_ws.append(_clean_row(sheet.row_values(r)))

            return clean_wb

        except xlrd.biffh.XLRDError:
            # Fake XLS → actually XLSX
            wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
            ws = wb.active

            clean_wb = openpyxl.Workbook()
            clean_ws = clean_wb.active

            for row in ws.iter_rows(values_only=True):
                clean_ws.append(list(_clean_row(row)))

            return clean_wb

    raise ValueError("Unsupported file type")


# ==================================================================
# CLEAN VALUE FIXER (DOB fix, scientific notation fix)
# ==================================================================
def _clean_row(row):
    cleaned = []
    for cell in row:
        if cell is None:
            cleaned.append(None)
            continue

        # Fix scientific notation numbers
        if isinstance(cell, float) and "e" in str(cell).lower():
            cleaned.append(str(int(cell)))
            continue

        # Fix DOB formats
        if isinstance(cell, float):  # Excel serial date
            try:
                dt = datetime.fromordinal(datetime(1899, 12, 30).toordinal() + int(cell))
                cleaned.append(dt.strftime("%Y-%m-%d"))
                continue
            except:
                pass

        cleaned.append(cell)
    return cleaned


# ==================================================================
# BOT HANDLERS
# ==================================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Send an Excel file (.xls or .xlsx)\nI will clean it, fix malformed data, "
        "and help you filter interactively."
    )
    return WAITING_FILE


async def receive_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        doc = update.message.document
        name = doc.file_name

        if not (name.endswith(".xls") or name.endswith(".xlsx")):
            await update.message.reply_text("Send only .xls or .xlsx")
            return WAITING_FILE

        tg_file = await context.bot.get_file(doc.file_id)
        file_bytes = await tg_file.download_as_bytearray()

        wb = load_excel_clean(bytes(file_bytes), name)
        ws = wb.active

        headers = []
        for c in range(1, ws.max_column + 1):
            h = ws.cell(1, c).value
            headers.append(f"{c}. {h if h else 'Column ' + str(c)}")

        context.user_data["wb"] = wb
        context.user_data["name"] = name
        context.user_data["max_col"] = ws.max_column

        await update.message.reply_text(
            "File loaded.\nColumns:\n" +
            "\n".join(headers) +
            "\n\nEnter column number:"
        )
        return WAITING_COLUMN

    except Exception as e:
        logger.error(e)
        await update.message.reply_text("Error reading file")
        return WAITING_FILE


async def receive_column(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        col = int(update.message.text.strip())
        if not 1 <= col <= context.user_data["max_col"]:
            await update.message.reply_text("Invalid column number")
            return WAITING_COLUMN

        context.user_data["col"] = col
        await update.message.reply_text(
            "Enter search text (I will show similar values):"
        )
        return WAITING_SEARCH

    except:
        await update.message.reply_text("Enter valid number")
        return WAITING_COLUMN


async def receive_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    search = update.message.text.strip().lower()
    col = context.user_data["col"]
    ws = context.user_data["wb"].active

    values = set()
    for r in range(2, ws.max_row + 1):
        v = ws.cell(r, col).value
        if v:
            values.add(str(v).strip())

    # Fuzzy match
    matches = process.extract(
        search, list(values),
        scorer=fuzz.WRatio,
        limit=10
    )

    top = [m[0] for m in matches if m[1] >= 60]

    if not top:
        await update.message.reply_text("No similar values found. Try again:")
        return WAITING_SEARCH

    msg = "Found similar values:\n\n"
    for i, v in enumerate(top, 1):
        msg += f"{i}. {v}\n"

    msg += "\nSend the number of the correct value:"
    context.user_data["options"] = top

    await update.message.reply_text(msg)
    return WAITING_VALUE_SELECT


async def receive_value_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        idx = int(update.message.text.strip()) - 1
        options = context.user_data["options"]

        if idx < 0 or idx >= len(options):
            await update.message.reply_text("Invalid choice")
            return WAITING_VALUE_SELECT

        context.user_data["value"] = options[idx]
        await update.message.reply_text(f"Filtering by: {options[idx]}")
        return await filter_and_send(update, context)

    except:
        await update.message.reply_text("Enter valid number")
        return WAITING_VALUE_SELECT


async def filter_and_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    col = context.user_data["col"]
    value = context.user_data["value"]
    ws = context.user_data["wb"].active

    new_wb = openpyxl.Workbook()
    new_ws = new_wb.active

    # header
    header = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]
    new_ws.append(header)

    count = 0
    for r in range(2, ws.max_row + 1):
        if str(ws.cell(r, col).value).strip() == value:
            row = [ws.cell(r, c).value for c in range(1, ws.max_column + 1)]
            new_ws.append(row)
            count += 1

    output = BytesIO()
    new_wb.save(output)
    output.seek(0)

    base = os.path.splitext(context.user_data["name"])[0]
    fname = f"{base}_filtered.xlsx"

    await update.message.reply_document(
        document=output,
        filename=fname,
        caption=f"Done. {count} matches."
    )
    return ConversationHandler.END


# ==================================================================
# APPLICATION
# ==================================================================
TOKEN = os.getenv("TOKEN")
application = Application.builder().token(TOKEN).updater(None).build()

conv = ConversationHandler(
    entry_points=[CommandHandler("start", start)],
    states={
        WAITING_FILE: [MessageHandler(filters.Document.ALL, receive_file)],
        WAITING_COLUMN: [MessageHandler(filters.TEXT, receive_column)],
        WAITING_SEARCH: [MessageHandler(filters.TEXT, receive_search)],
        WAITING_VALUE_SELECT: [MessageHandler(filters.TEXT, receive_value_select)],
    },
    fallbacks=[]
)

application.add_handler(conv)
