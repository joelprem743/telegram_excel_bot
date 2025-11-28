import os
import logging
from telegram import Update, ForceReply
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler
)
import openpyxl
from openpyxl.utils import get_column_letter
from io import BytesIO

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# States for conversation
WAITING_FILE, WAITING_COLUMN, WAITING_VALUE = range(3)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the conversation and ask for Excel file."""
    await update.message.reply_text(
        "Hi! I'll help you filter Excel files.\n\n"
        "📊 Send me an Excel file (.xlsx format only)\n\n"
        "💡 If you have a .xls file, please convert it to .xlsx first."
    )
    return WAITING_FILE

async def receive_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Receive and store the Excel file."""
    try:
        document = update.message.document
        
        # Check file extension - only accept .xlsx
        file_ext = document.file_name.lower()
        if not file_ext.endswith('.xlsx'):
            await update.message.reply_text(
                "⚠️ Please send an Excel file in .xlsx format.\n\n"
                "If you have a .xls file:\n"
                "1. Open it in Excel/LibreOffice/Google Sheets\n"
                "2. Save As → Excel Workbook (.xlsx)\n"
                "3. Upload the .xlsx file here"
            )
            return WAITING_FILE
        
        # Download file
        file = await context.bot.get_file(document.file_id)
        file_bytes = await file.download_as_bytearray()
        
        # Store in context
        context.user_data['file_bytes'] = bytes(file_bytes)
        context.user_data['file_name'] = document.file_name
        
        # Load with openpyxl
        wb = openpyxl.load_workbook(BytesIO(context.user_data['file_bytes']))
        ws = wb.active
        
        headers = []
        for col in range(1, ws.max_column + 1):
            cell_value = ws.cell(1, col).value
            if cell_value:
                headers.append(f"{col}. {cell_value}")
            else:
                headers.append(f"{col}. Column {get_column_letter(col)}")
        
        context.user_data['headers'] = headers
        context.user_data['max_column'] = ws.max_column
        
        headers_text = "\n".join(headers)
        await update.message.reply_text(
            f"File received! Found {context.user_data['max_column']} columns:\n\n{headers_text}\n\n"
            f"Enter the column number you want to filter by:"
        )
        
        return WAITING_COLUMN
        
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        await update.message.reply_text(
            f"Error processing file: {str(e)}\n"
            "Please try again with a valid Excel file."
        )
        return WAITING_FILE

async def receive_column(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Receive the column number to filter."""
    try:
        column_num = int(update.message.text.strip())
        
        if column_num < 1 or column_num > context.user_data['max_column']:
            await update.message.reply_text(
                f"Invalid column number. Please enter a number between 1 and {context.user_data['max_column']}:"
            )
            return WAITING_COLUMN
        
        context.user_data['filter_column'] = column_num
        
        await update.message.reply_text(
            f"Great! Now enter the value you want to filter for in column {column_num}:"
        )
        
        return WAITING_VALUE
        
    except ValueError:
        await update.message.reply_text(
            "Please enter a valid number:"
        )
        return WAITING_COLUMN

async def receive_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Filter the Excel file based on the value."""
    try:
        filter_value = update.message.text.strip()
        context.user_data['filter_value'] = filter_value
        
        await update.message.reply_text("Processing your file... Please wait.")
        
        filter_col = context.user_data['filter_column']
        
        # Use openpyxl for all files (xls was converted earlier)
        wb = openpyxl.load_workbook(BytesIO(context.user_data['file_bytes']))
        ws = wb.active
        
        # Find matching rows (skip header)
        rows_to_keep = [1]  # Always keep header
        matched_count = 0
        
        for row_idx in range(2, ws.max_row + 1):
            cell_value = ws.cell(row_idx, filter_col).value
            
            # Convert both to string for comparison
            if cell_value is not None and str(cell_value).strip().lower() == filter_value.lower():
                rows_to_keep.append(row_idx)
                matched_count += 1
        
        if matched_count == 0:
            await update.message.reply_text(
                f"No records found matching '{filter_value}' in the selected column.\n\n"
                "Send /start to try again."
            )
            return ConversationHandler.END
        
        # Create new workbook with filtered data
        new_wb = openpyxl.Workbook()
        new_ws = new_wb.active
        new_ws.title = "Filtered Data"
        
        # Copy rows that match
        for new_row_idx, old_row_idx in enumerate(rows_to_keep, start=1):
            for col_idx in range(1, ws.max_column + 1):
                old_cell = ws.cell(old_row_idx, col_idx)
                new_cell = new_ws.cell(new_row_idx, col_idx)
                
                # Copy value
                new_cell.value = old_cell.value
                
                # Copy formatting
                if old_cell.has_style:
                    new_cell.font = old_cell.font.copy()
                    new_cell.border = old_cell.border.copy()
                    new_cell.fill = old_cell.fill.copy()
                    new_cell.number_format = old_cell.number_format
                    new_cell.protection = old_cell.protection.copy()
                    new_cell.alignment = old_cell.alignment.copy()
        
        # Auto-adjust column widths based on content
        for col_idx in range(1, ws.max_column + 1):
            col_letter = get_column_letter(col_idx)
            
            # Try to get original width first
            original_width = None
            if col_letter in ws.column_dimensions:
                original_width = ws.column_dimensions[col_letter].width
            
            # Calculate optimal width based on content
            max_length = 0
            for row_idx in rows_to_keep:
                cell_value = ws.cell(row_idx, col_idx).value
                if cell_value:
                    cell_length = len(str(cell_value))
                    max_length = max(max_length, cell_length)
            
            # Set width (use original if available, otherwise auto-calculate)
            if original_width and original_width > 8:
                new_ws.column_dimensions[col_letter].width = original_width
            elif max_length > 0:
                adjusted_width = min(max_length + 2, 50)  # Cap at 50
                new_ws.column_dimensions[col_letter].width = adjusted_width
            else:
                new_ws.column_dimensions[col_letter].width = 12  # Default width
        
        # Copy row heights
        for new_row_idx, old_row_idx in enumerate(rows_to_keep, start=1):
            if old_row_idx in ws.row_dimensions:
                old_height = ws.row_dimensions[old_row_idx].height
                if old_height:
                    new_ws.row_dimensions[new_row_idx].height = old_height
        
        # Save to bytes
        output = BytesIO()
        new_wb.save(output)
        output.seek(0)
        
        # Generate output filename
        original_name = context.user_data['file_name']
        base_name = os.path.splitext(original_name)[0]
        output_name = f"{base_name}_filtered.xlsx"
        
        # Send filtered file
        await update.message.reply_document(
            document=output,
            filename=output_name,
            caption=f"✅ Filtered complete!\n\n"
                    f"Found {matched_count} records matching '{filter_value}'\n"
                    f"Send /start to filter another file."
        )
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Error filtering file: {e}")
        await update.message.reply_text(
            f"Error filtering file: {str(e)}\n"
            "Send /start to try again."
        )
        return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel the conversation."""
    await update.message.reply_text(
        "Operation cancelled. Send /start to begin again."
    )
    return ConversationHandler.END

def main() -> None:
    """Start the bot."""
    # Replace 'YOUR_BOT_TOKEN' with your actual bot token from @BotFather
    from dotenv import load_dotenv
    import os

    load_dotenv()  # loads .env from the project root

    BOT_TOKEN = os.getenv("TOKEN")
    
    
    # Create application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Conversation handler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            WAITING_FILE: [
                MessageHandler(filters.Document.ALL, receive_file)
            ],
            WAITING_COLUMN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_column)
            ],
            WAITING_VALUE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_value)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    
    application.add_handler(conv_handler)
    
    # Start the bot
    logger.info("Bot started...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()