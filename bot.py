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
# NORMALIZATION FUNCTION (fixed logic)
# -------------------------------------------------------
def normalize(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)

    t = text.lower().strip()

    # Remove brackets but preserve their contents
    # "(10014283)" -> "10014283"
    t = re.sub(r'[\(\)]', ' ', t)

    # Replace non-alphanumeric with space
    t = re.sub(r'[^a-z0-9]+', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()

    return t


# -------------------------------------------------------
# Create safe output filename
# -------------------------------------------------------
def make_safe_filename(text: str) -> str:
    t = normalize(text)
    t = t.replace(" ", "-")
    return t


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Send me an Excel file (.xlsx or .xls) to begin.")
    return ConversationHandler.END


async def handle_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    file = update.message.document
    filename = file.file_name.lower()

    # Only allow Excel files
    if not (filename.endswith(".xlsx") or filename.endswith(".xls")):
        await update.message.reply_text("Send only Excel files (.xlsx or .xls).")
        return ConversationHandler.END

    user_id = update.effective_user.id

    # Create temp folder for this user
    temp_dir = f"/tmp/{user_id}"
    os.makedirs(temp_dir, exist_ok=True)

    # Download uploaded file into the user-specific temp directory
    file_path = f"{temp_dir}/{file.file_name}"
    new_file = await file.get_file()
    await new_file.download_to_drive(file_path)

    user_file_cache[user_id] = file_path

    await update.message.reply_text("Excel received. Enter the VALUE to filter:")
    return ASK_VALUE


async def ask_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    value_input = update.message.text.strip()
    norm_value_input = normalize(value_input)

    user_id = update.effective_user.id
    file_path = user_file_cache[user_id]

    temp_dir = os.path.dirname(file_path)

    try:
        # ---------------------------
        # FIXED ENGINE SELECTION
        # ---------------------------
        if file_path.lower().endswith(".xlsx"):
            excel_data = pd.read_excel(file_path, sheet_name=None, engine="openpyxl")
        else:
            excel_data = pd.read_excel(file_path, sheet_name=None, engine="xlrd")

        safe_name = make_safe_filename(value_input)
        output_path = f"{temp_dir}/{safe_name}.xlsx"

        writer = pd.ExcelWriter(output_path, engine="openpyxl")

        for sheet, df in excel_data.items():
            df_norm = df.astype(str).applymap(normalize)

            # Row matches if ANY cell contains the normalized search term
            mask = df_norm.apply(
                lambda row: any(norm_value_input in cell for cell in row),
                axis=1
            )

            df_filtered = df[mask]
            df_filtered.to_excel(writer, sheet_name=sheet, index=False)

        writer.close()

        # Send file to user
        await update.message.reply_document(
            document=open(output_path, "rb"),
            filename=f"{safe_name}.xlsx"
        )

    except Exception as e:
        await update.message.reply_text(f"Error processing file: {str(e)}")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    return ConversationHandler.END


def main():
    TOKEN = os.getenv("TOKEN")

    application = ApplicationBuilder().token(TOKEN).build()
    excel_filter = filters.Document.ALL

    conv = ConversationHandler(
        entry_points=[MessageHandler(excel_filter, handle_excel)],
        states={
            ASK_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_value)],
        },
        fallbacks=[CommandHandler("start", start)],
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(conv)

    application.run_polling()


if __name__ == "__main__":
    main()
