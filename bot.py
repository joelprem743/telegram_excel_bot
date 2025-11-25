import logging
import os
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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Send me an Excel file (.xlsx or .xls) to begin.")
    return ConversationHandler.END


async def handle_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    file = update.message.document
    filename = file.file_name.lower()

    if not (filename.endswith(".xlsx") or filename.endswith(".xls")):
        await update.message.reply_text("Send only Excel files (.xlsx or .xls).")
        return ConversationHandler.END

    user_id = update.effective_user.id

    temp_dir = f"/tmp/{user_id}"
    os.makedirs(temp_dir, exist_ok=True)

    file_path = f"{temp_dir}/{file.file_name}"
    tg_file = await file.get_file()
    await tg_file.download_to_drive(file_path)

    user_file_cache[user_id] = file_path

    await update.message.reply_text("Excel received. Enter the VALUE to search (loose match):")
    return ASK_VALUE


async def ask_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    value_input = update.message.text.strip()

    user_id = update.effective_user.id
    file_path = user_file_cache[user_id]
    temp_dir = os.path.dirname(file_path)

    try:
        if file_path.lower().endswith(".xlsx"):
            excel_data = pd.read_excel(file_path, sheet_name=None, engine="openpyxl")
        else:
            excel_data = pd.read_excel(file_path, sheet_name=None, engine="xlrd")

        safe_name = value_input.replace(" ", "_").replace("-", "_")
        output_path = f"{temp_dir}/{safe_name}.xlsx"

        writer = pd.ExcelWriter(output_path, engine="openpyxl")

        # Normalize user input
        normalized_input = value_input.lower().replace(" ", "").replace("-", "")

        def normalize(s):
            return s.lower().replace(" ", "").replace("-", "")

        for sheet, df in excel_data.items():
            df_str = df.astype(str)

            # Loose substring match in ANY cell of the row
            mask = df_str.apply(
                lambda row: any(normalized_input in normalize(cell) for cell in row),
                axis=1
            )

            df_filtered = df[mask]
            df_filtered.to_excel(writer, sheet_name=sheet, index=False)

        writer.close()

        await update.message.reply_document(
            document=open(output_path, "rb"),
            filename=f"{safe_name}.xlsx"
        )

    except Exception as e:
        await update.message.reply_text(f"Error processing file: {str(e)}")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    return ConversationHandler.END


def simple_env_loader():
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            for line in f:
                if "=" in line:
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value


def main():
    # Custom environment loader since python-dotenv doesn't support Python 3.14 yet
    simple_env_loader()

    TOKEN = os.getenv("TOKEN")
    print("TOKEN RAW:", repr(TOKEN))

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
