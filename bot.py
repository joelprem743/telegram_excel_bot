# bot.py
import os
import logging
from io import BytesIO
from datetime import datetime, timedelta
from typing import List, Any
from dotenv import load_dotenv

from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters,
    ContextTypes, ConversationHandler
)

import openpyxl
import xlrd
from rapidfuzz import process, fuzz  # for ranking found values (optional but recommended)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Conversation states
WAITING_FILE, WAITING_COLUMN, WAITING_QUERY, WAITING_SELECT = range(4)

# --- Utility helpers -------------------------------------------------------
def is_integer_like_number(v: Any) -> bool:
    try:
        if isinstance(v, float):
            return v.is_integer()
        if isinstance(v, int):
            return True
    except Exception:
        return False
    return False

def format_number_no_scientific(v: Any) -> str:
    """
    Convert large numeric floats/ints to human-readable integer strings to avoid scientific notation.
    Leaves non-numeric values untouched.
    """
    if v is None:
        return ""
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if v.is_integer():
            return str(int(v))
        else:
            # For non-integer floats, keep as-is but avoid scientific notation
            return format(v, "f").rstrip("0").rstrip(".")
    # if it's string already, just return
    return str(v)

def parse_possible_date(value: Any):
    """
    Try to detect if value is a date (datetime, excel serial float, or common date-like string).
    Returns a datetime on success, else None.
    """
    if value is None:
        return None

    # If it's already a datetime
    if isinstance(value, datetime):
        return value

    # If it's a float/int numeric that might be an Excel serial (e.g., 45000)
    # We'll treat large integers as not dates (e.g., RCHID). Heuristic:
    # typical Excel serial for dates (1900 system) are numbers ~ 20000-50000 (2000-2037)
    try:
        if isinstance(value, (int, float)):
            # ignore extremely large numbers that are likely IDs (e.g. > 1e6)
            if 1000 < value < 100000:
                # try Excel 1900 serial -> python datetime
                try:
                    # xlrd and openpyxl use different base; here we use xlrd's conversion
                    # but only if xlrd is available
                    # Note: if the original was loaded via xlrd it may already be converted earlier.
                    dt = xlrd.xldate.xldate_as_datetime(value, 0)  # 0==1900-based
                    return dt
                except Exception:
                    # fallback: try to interpret as ordinal (datetime.fromordinal won't be correct)
                    pass
    except Exception:
        pass

    # If it's a string, attempt parsing with common patterns
    if isinstance(value, str):
        s = value.strip()
        # common formats to attempt:
        fmts = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%Y/%m/%d",
            "%d-%b-%Y",
            "%d %b %Y",
            "%m/%d/%Y",
            "%Y.%m.%d %H:%M:%S",
            "%Y.%m.%d"
        ]
        for f in fmts:
            try:
                return datetime.strptime(s, f)
            except Exception:
                continue
        # try parsing ISOlike '2024-10-04T00:00:00' without timezone
        try:
            if "t" in s.lower():
                return datetime.fromisoformat(s)
        except Exception:
            pass

    return None

def format_cell_for_output(value: Any) -> Any:
    """
    Convert a cell value to a safe representation for writing into the output Excel:
    - dates -> DD-MM-YYYY string
    - large numeric ids -> integer string (no scientific)
    - other -> kept as-is
    """
    if value is None:
        return None

    # If value is datetime
    if isinstance(value, datetime):
        return value.strftime("%d-%m-%Y")

    # If value is numeric but likely an ID or phone number -> string without scientific notation
    if isinstance(value, (int, float)) and (is_integer_like_number(value)):
        # If it looks small (< 1e6) it might be a normal number; still safe to convert to int
        v_int = int(value)
        # Heuristic: if v_int looks like a date ordinal (less than 100000) we shouldn't convert blindly
        # We'll attempt to detect date first, above.
        return str(v_int)

    # Try parse string-as-date
    possible_date = parse_possible_date(value)
    if possible_date:
        return possible_date.strftime("%d-%m-%Y")

    # Fallback for strings: remove non-printable, strip
    if isinstance(value, str):
        return value.strip()

    # For other types, convert to string
    return str(value)

# --- Excel loader that returns a clean openpyxl workbook --------------------
def load_excel_clean(file_bytes: bytes, file_name: str) -> openpyxl.Workbook:
    """
    Load given bytes from .xls or .xlsx into an openpyxl Workbook containing only raw values.
    Handles real .xls (via xlrd), real .xlsx (openpyxl) and mis-labeled '.xls' that are xlsx files.
    """
    lower = file_name.lower()
    if lower.endswith(".xlsx"):
        wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
        ws = wb.active
        clean_wb = openpyxl.Workbook()
        clean_ws = clean_wb.active
        for row in ws.iter_rows(values_only=True):
            clean_ws.append([cell for cell in row])
        return clean_wb

    if lower.endswith(".xls"):
        # try reading with xlrd first
        try:
            book = xlrd.open_workbook(file_contents=file_bytes)
            sheet = book.sheet_by_index(0)
            clean_wb = openpyxl.Workbook()
            clean_ws = clean_wb.active
            for r in range(sheet.nrows):
                # xlrd returns numbers/dates as floats/tuples; we keep raw values for later conversion
                clean_ws.append(sheet.row_values(r))
            return clean_wb
        except xlrd.biffh.XLRDError as e:
            # If xlrd complains "Excel xlsx file; not supported" attempt to load as xlsx (file mislabeled)
            if "xlsx file; not supported" in str(e).lower():
                wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
                ws = wb.active
                clean_wb = openpyxl.Workbook()
                clean_ws = clean_wb.active
                for row in ws.iter_rows(values_only=True):
                    clean_ws.append([cell for cell in row])
                return clean_wb
            # otherwise re-raise
            raise

    raise ValueError("Unsupported file type. Send .xls or .xlsx")

# --- Bot handlers ----------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Send an Excel file (.xls or .xlsx). I'll strip formatting and work with values-only.\n\n"
        "Flow:\n"
        "1) You upload file\n"
        "2) You pick a column number\n"
        "3) You type a partial search string (e.g. 'avanigadda')\n"
        "4) I show matching distinct values from that column and you pick the exact entry\n"
        "5) I return an .xlsx with header + all rows that contain the chosen entry (substring match)\n\n"
        "Send /cancel at any time to stop."
    )
    return WAITING_FILE

async def receive_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        doc = update.message.document
        if not doc:
            await update.message.reply_text("No file detected. Send an Excel document (.xls or .xlsx).")
            return WAITING_FILE

        fname = doc.file_name
        if not (fname.lower().endswith(".xls") or fname.lower().endswith(".xlsx")):
            await update.message.reply_text("Please send only .xls or .xlsx files.")
            return WAITING_FILE

        tgfile = await context.bot.get_file(doc.file_id)
        fb = await tgfile.download_as_bytearray()

        # Load clean workbook
        wb = load_excel_clean(bytes(fb), fname)
        ws = wb.active

        # Save to user_data
        context.user_data["wb"] = wb
        context.user_data["file_name"] = fname
        context.user_data["max_col"] = ws.max_column

        # Build simple column list (1-indexed)
        headers = []
        for col in range(1, ws.max_column + 1):
            v = ws.cell(1, col).value
            if v is None:
                headers.append(f"{col}. Column {openpyxl.utils.get_column_letter(col)}")
            else:
                headers.append(f"{col}. {str(v)}")

        await update.message.reply_text(
            "File loaded. Detected columns:\n\n" + "\n".join(headers) +
            f"\n\nReply with the column *number* you want to search (1 - {ws.max_column}).",
            parse_mode="Markdown"
        )
        return WAITING_COLUMN
    except Exception as e:
        logger.exception("Error in receive_file")
        await update.message.reply_text(f"Error reading file: {e}\nSend a valid .xls or .xlsx.")
        return WAITING_FILE

async def receive_column(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip()
    try:
        c = int(txt)
    except Exception:
        await update.message.reply_text("Please send a valid column number.")
        return WAITING_COLUMN

    if "max_col" not in context.user_data or c < 1 or c > context.user_data["max_col"]:
        await update.message.reply_text(f"Invalid column number. Enter 1 to {context.user_data.get('max_col','?')}.")
        return WAITING_COLUMN

    context.user_data["col"] = c
    await update.message.reply_text(
        "Enter a search string (partial match). I will show all distinct column values that contain this substring."
    )
    return WAITING_QUERY

async def receive_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.strip()
    if not query:
        await update.message.reply_text("Send a non-empty search string.")
        return WAITING_QUERY

    wb: openpyxl.Workbook = context.user_data["wb"]
    ws = wb.active
    col = context.user_data["col"]

    # collect distinct values from column
    seen = {}
    for r in range(2, ws.max_row + 1):
        raw = ws.cell(r, col).value
        if raw is None:
            continue
        # create normalized key for matching (lower)
        norm = str(raw).strip().lower()
        if query.lower() in norm:
            if norm not in seen:
                seen[norm] = str(raw).strip()

    if not seen:
        await update.message.reply_text(f"No values in column {col} contain '{query}'. Try another search or /start again.")
        return ConversationHandler.END

    # Rank candidates: use rapidfuzz to sort best matches first (optional)
    choices = list(seen.values())
    # We rank by fuzzy partial_ratio against the query
    ranked = process.extract(query, choices, scorer=fuzz.partial_ratio, limit=200)
    # ranked is list of tuples (choice, score, index)
    ordered_choices = [t[0] for t in ranked]

    # limit the number of presented options
    MAX_CHOICES = 40
    presented = ordered_choices[:MAX_CHOICES]

    # store mapping to allow selection
    context.user_data["candidates"] = presented

    # prepare message chunked if many
    lines = [f"Found {len(seen)} distinct matching values. Showing top {len(presented)}:"]
    for i, v in enumerate(presented, start=1):
        lines.append(f"{i}. {v}")

    lines.append("\nReply with the number of the correct value (or 0 to cancel).")
    await update.message.reply_text("\n".join(lines))
    return WAITING_SELECT

async def receive_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip()
    if not txt.isdigit():
        await update.message.reply_text("Please reply with the number of the value you want (e.g. 2).")
        return WAITING_SELECT
    idx = int(txt)
    if idx == 0:
        await update.message.reply_text("Cancelled. Send /start to begin again.")
        return ConversationHandler.END

    candidates: List[str] = context.user_data.get("candidates", [])
    if idx < 1 or idx > len(candidates):
        await update.message.reply_text(f"Invalid selection. Choose a number between 1 and {len(candidates)}.")
        return WAITING_SELECT

    chosen = candidates[idx - 1]
    context.user_data["chosen_value"] = chosen

    # Now perform the filtering: rows where chosen is substring of the selected column cell (case-insensitive)
    wb: openpyxl.Workbook = context.user_data["wb"]
    ws = wb.active
    col = context.user_data["col"]

    # Prepare output workbook
    out_wb = openpyxl.Workbook()
    out_ws = out_wb.active
    out_ws.title = "Filtered"

    # copy header row while formatting header as string
    header_row = []
    for c in range(1, ws.max_column + 1):
        header_row.append(str(ws.cell(1, c).value) if ws.cell(1, c).value is not None else "")
    out_ws.append(header_row)

    match_count = 0
    for r in range(2, ws.max_row + 1):
        val = ws.cell(r, col).value
        if val is None:
            continue
        if chosen.strip().lower() in str(val).strip().lower():
            # copy entire row, formatting each cell
            formatted_row = [format_cell_for_output(ws.cell(r, c).value) for c in range(1, ws.max_column + 1)]
            out_ws.append(formatted_row)
            match_count += 1

    if match_count == 0:
        await update.message.reply_text(f"No rows found containing '{chosen}' — this is unexpected. Try /start.")
        return ConversationHandler.END

    # Ensure column widths are reasonable (basic heuristic)
    for c in range(1, ws.max_column + 1):
        letter = openpyxl.utils.get_column_letter(c)
        max_len = 0
        for row in out_ws.iter_rows(min_col=c, max_col=c):
            for cell in row:
                if cell.value is None:
                    continue
                l = len(str(cell.value))
                if l > max_len:
                    max_len = l
        out_ws.column_dimensions[letter].width = min(max(max_len + 2, 12), 60)

    # Save to BytesIO and send
    output = BytesIO()
    out_wb.save(output)
    output.seek(0)

    base = os.path.splitext(context.user_data.get("file_name", "filtered"))[0]
    out_name = f"{base}_filtered_by_{col}_{chosen[:30].replace(' ', '_')}.xlsx"

    await update.message.reply_document(
        document=output,
        filename=out_name,
        caption=f"Filtered file ready — {match_count} rows matched '{chosen}'."
    )
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Operation cancelled. Send /start to begin again.")
    return ConversationHandler.END

# --- Main -----------------------------------------------------------------
def main():
    load_dotenv() 
    TOKEN = os.getenv("TOKEN")
    if not TOKEN:
        logger.error("TOKEN env var not set. Set TOKEN in environment and restart.")
        raise SystemExit("TOKEN env var not set. Set TOKEN in environment and restart.")

    application = Application.builder().token(TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            WAITING_FILE: [MessageHandler(filters.Document.ALL, receive_file)],
            WAITING_COLUMN: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_column)],
            WAITING_QUERY: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_query)],
            WAITING_SELECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_select)]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True
    )

    application.add_handler(conv)

    logger.info("Bot starting (polling)...")
    application.run_polling()

if __name__ == "__main__":
    main()
