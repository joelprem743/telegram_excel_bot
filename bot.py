import logging
import os
import re
import shutil
import pandas as pd
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    CommandHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(level=logging.INFO)

ASK_VALUE = range(1)
user_file_cache = {}


# -------------------------------------------------------
# NORMALIZE: safe, consistent lowercase text
# -------------------------------------------------------
def normalize(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)

    t = text.lower().strip()

    # Remove parentheses but keep the contents
    t = re.sub(r'[\(\)]', ' ', t)

    # Replace non-alphanumeric with spaces
    t = re.sub(r'[^a-z0-9]+', ' ', t)

    # Collapse spaces
    t = re.sub(r'\s+', ' ', t).strip()

    return t


# -------------------------------------------------------
# Check if a '2' exists OUTSIDE parentheses in original text
# -------------------------------------------------------
def has_2_outside_brackets(original: str) -> bool:
    if not isinstance(original, str):
        original = str(original)

    for match in re.finditer("2", original):
        idx = match.start()
        open_before = original[:idx].count("(")
        close_before = original[:idx].count(")")
        if open_before == close_before:  # only true if outside parentheses
            return True

    return False


# -------------------------------------------------------
# STRICT EXACT MATCH LOGIC
# -------------------------------------------------------
def is_exact_valid_cell(original_cell: str, normalized_cell: str) -> bool:
    # Must contain a '2' outside parentheses
    if not has_2_outside_brackets(original_cell):
        return False

    # Strict regex patterns (exact entire cell match)
    patterns = [
        r"^avanigadda-? ?2$",            # avanigadda2, avanigadda 2, avanigadda-2
        r"^sc-? ?avanigadda-? ?2$",      # sc avanigadda 2, scavanigadda2, sc-avanigadda-2
    ]

    for p in patterns:
        if re.fullmatch(p, normalized_cell):
            return True

    return False


# -------------------------------------------------------
# Safe filename
# -------------------------------------------------------
def make_safe_filename(text: str) -> str:
    t = normalize(text)
    return t.replace(" ", "-")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Send an Excel file (.xlsx or .xls).")
    return ConversationHandler.END


async def handle_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    file = update.message.document
    filename = file.file_name.lower()

    if not (filename.endswith(".xlsx") or filename.endswith(".xls")):
        await update.message.reply_text("Only .xlsx or .xls allowed.")
        return ConversationHandler.END

    user_id = update.effective_user.id
    temp_dir = f"/tmp/{user_id}"
    os.makedirs(temp_dir, exist_ok=True)

    file_path = f"{temp_dir}/{file.file_name}"
    f = await file.get_file()
    await f.download_to_drive(file_path)

    user_file_cache[user_id] = file_path

    await update.message.reply_text("Excel received. Send ANY text to continue filtering.")
    return ASK_VALUE


async def ask_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    file_path = user_file_cache[user_id]
    temp_dir = os.path.dirname(file_path)

    try:
        # Pick engine
        if file_path.endswith(".xlsx"):
            excel_data = pd.read_excel(file_path, sheet_name=None, engine="openpyxl")
        else:
            excel_data = pd.read_excel(file_path, sheet_name=None, engine="xlrd")

        output_path = f"{temp_dir}/avanigadda-2-exact.xlsx"
        writer = pd.ExcelWriter(output_path, engine="openpyxl")

        for sheet, df in excel_data.items():
            df_str = df.astype(str)
            df_norm = df_str.applymap(normalize)

            matches = []

            for idx, row in df_str.iterrows():
                found = False

                for original, norm in zip(row, df_norm.loc[idx]):
                    if is_exact_valid_cell(original, norm):
                        found = True
                        break

                matches.append(found)

            df_filtered = df[matches]
            df_filtered.to_excel(writer, sheet_name=sheet, index=False)

        writer.close()

        await update.message.reply_document(
            document=open(output_path, "rb"),
            filename="avanigadda-2-exact.xlsx"
        )

    except Exception as e:
        await update.message.reply_text(f"Error: {e}")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    return ConversationHandler.END


def main():
    TOKEN = os.getenv("TOKEN")
    application = ApplicationBuilder().token(TOKEN).build()

    conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Document.ALL, handle_excel)],
        states={ASK_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_value)]},
        fallbacks=[CommandHandler("start", start)],
    )

    application.add_handler(conv)
    application.add_handler(CommandHandler("start", start))

    application.run_polling()


if __name__ == "__main__":
    main()
