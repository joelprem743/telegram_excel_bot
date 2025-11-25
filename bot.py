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


# Normalize text for pattern matching
def normalize(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)
    t = text.lower().strip()
    t = re.sub(r'[\(\)]', lambda m: ' ' if m.group() in '()' else m.group(), t)
    t = re.sub(r'[^a-z0-9]+', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


# Detect a "2" outside parentheses
def has_2_outside_brackets(original_text: str) -> bool:
    if not isinstance(original_text, str):
        original_text = str(original_text)

    for match in re.finditer("2", original_text):
        idx = match.start()
        open_before = original_text[:idx].count("(")
        close_before = original_text[:idx].count(")")
        if open_before == close_before:
            return True
    return False


# EXACT strict cell match using flexible patterns
def matches_strict_pattern(original_cell: str, normalized_cell: str) -> bool:

    if not has_2_outside_brackets(original_cell):
        return False

    patterns = [
        r"^avanigadda[\s-]?2(\(\d+\))?$",
        r"^sc[\s-]?avanigadda[\s-]?2(\(\d+\))?$"
    ]

    for p in patterns:
        if re.fullmatch(p, normalized_cell):
            return True
    return False


def make_safe_filename(text: str) -> str:
    t = normalize(text)
    return t.replace(" ", "-")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Send me an Excel file (.xlsx or .xls).")
    return ConversationHandler.END


async def handle_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    file = update.message.document
    filename = file.file_name.lower()

    if not (filename.endswith(".xlsx") or filename.endswith(".xls")):
        await update.message.reply_text("Only Excel files allowed.")
        return ConversationHandler.END

    user_id = update.effective_user.id
    temp_dir = f"/tmp/{user_id}"
    os.makedirs(temp_dir, exist_ok=True)

    file_path = f"{temp_dir}/{file.file_name}"
    new_file = await file.get_file()
    await new_file.download_to_drive(file_path)

    user_file_cache[user_id] = file_path
    await update.message.reply_text("Excel received. Send any message to run filter:")

    return ASK_VALUE


async def ask_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    file_path = user_file_cache[user_id]
    temp_dir = os.path.dirname(file_path)

    try:
        if file_path.lower().endswith(".xlsx"):
            excel_data = pd.read_excel(file_path, sheet_name=None, engine="openpyxl")
        else:
            excel_data = pd.read_excel(file_path, sheet_name=None, engine="xlrd")

        output_path = f"{temp_dir}/avanigadda-2-filtered-flex.xlsx"
        writer = pd.ExcelWriter(output_path, engine="openpyxl")

        for sheet, df in excel_data.items():
            df_str = df.astype(str)
            df_norm = df_str.applymap(normalize)

            matches = []

            for idx, row in df_str.iterrows():
                found = False
                for orig, norm in zip(row, df_norm.loc[idx]):
                    if matches_strict_pattern(orig, norm):
                        found = True
                        break
                matches.append(found)

            df_filtered = df[matches]
            df_filtered.to_excel(writer, sheet_name=sheet, index=False)

        writer.close()

        await update.message.reply_document(
            document=open(output_path, "rb"),
            filename="avanigadda-2-filtered-flex.xlsx"
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
