import asyncio
import gspread
import os
import logging
import time
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
)

# --- Configuration ---
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE", "credentials.json")
SPREADSHEET_NAME = "Анкета для батьків від MamasRestHub (Відповіді)"
WORKSHEET_NAME = "Відповіді форми (1)"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "7728439258:AAGSmJMOlz72GvG0Q40gakUurUuU1JdFukc")
CHAT_IDS = os.getenv("CHAT_IDS", "105162170,431530966,221693560").split(",")
POLLING_INTERVAL = 300  # Check every 5 minutes (300 seconds)

# --- State ---
bot_running = False
last_row_count = 0
headers = None
api_request_count = 0

# --- Logging ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Helper Functions ---
def log_message(message):
    logger.info(message)

def escape_markdown_v2(text):
    """Escape special characters for MarkdownV2 parsing."""
    if not isinstance(text, str):
        text = str(text)
    special_chars = r'_[]()~`>#+-={}.!?:'
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text

def escape_html(text):
    """Escape special characters for HTML parsing."""
    if not isinstance(text, str):
        text = str(text)
    return text.replace('&', '&').replace('<', '<').replace('>', '>')

async def fetch_with_backoff(func, *args, max_retries=3, initial_delay=5):
    """Execute a gspread function with exponential backoff on rate limit errors."""
    global api_request_count
    for attempt in range(max_retries):
        try:
            api_request_count += 1
            log_message(f"API request count: {api_request_count}")
            return await asyncio.get_event_loop().run_in_executor(None, lambda: func(*args))
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429:  # Rate limit exceeded
                delay = initial_delay * (2 ** attempt)
                log_message(f"Rate limit exceeded. Retrying in {delay} seconds (attempt {attempt + 1}/{max_retries})...")
                await asyncio.sleep(delay)
            else:
                raise
    raise Exception(f"Failed to execute {func.__name__} after {max_retries} retries due to rate limiting.")

# --- Initialize Headers ---
def initialize_headers():
    global headers
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPE)
    client = gspread.authorize(creds)
    sheet = client.open(SPREADSHEET_NAME).worksheet(WORKSHEET_NAME)
    headers = sheet.row_values(1)
    log_message("✅ Headers initialized.")

# --- Monitor Google Sheet ---
async def monitor_sheet(app_context: ContextTypes.DEFAULT_TYPE):
    global bot_running, last_row_count, headers
    log_message("✅ Sheet monitor started.")

    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPE)
    client = gspread.authorize(creds)
    sheet = client.open(SPREADSHEET_NAME).worksheet(WORKSHEET_NAME)

    if headers is None:
        headers = await fetch_with_backoff(sheet.row_values, 1)

    while bot_running:
        try:
            log_message("🔁 Checking for new entries...")
            all_rows = await fetch_with_backoff(sheet.get_all_values)
            current_row_count = len(all_rows) - 1

            if current_row_count > last_row_count:
                new_entries = all_rows[last_row_count + 1:]
                for row in new_entries:
                    msg_md = "\n".join(
                        f"*{escape_markdown_v2(headers[i] if i < len(headers) else f'Column {i+1}')}:* {escape_markdown_v2(cell)}"
                        for i, cell in enumerate(row)
                    )
                    for chat_id in CHAT_IDS:
                        try:
                            await app_context.bot.send_message(
                                chat_id=chat_id,
                                text=f"*New Entry:*\n{msg_md}",
                                parse_mode="MarkdownV2"
                            )
                        except Exception as e:
                            if "bot can't initiate conversation" in str(e):
                                log_message(f"Cannot send message to chat_id {chat_id}: User has not initiated conversation with the bot.")
                            elif "bot was blocked" in str(e):
                                log_message(f"Cannot send message to chat_id {chat_id}: Bot was blocked by the user.")
                            else:
                                log_message(f"MarkdownV2 failed for row {row} (chat_id {chat_id}): {e}")
                                msg_html = "\n".join(
                                    f"<b>{escape_html(headers[i] if i < len(headers) else f'Column {i+1}')}</b>: {escape_html(cell)}"
                                    for i, cell in enumerate(row)
                                )
                                try:
                                    await app_context.bot.send_message(
                                        chat_id=chat_id,
                                        text=f"<b>New Entry:</b>\n{msg_html}",
                                        parse_mode="HTML"
                                    )
                                except Exception as send_err:
                                    log_message(f"HTML fallback failed for chat_id {chat_id}: {send_err}")
                last_row_count = current_row_count
                log_message(f"✅ Sent {len(new_entries)} new row(s). Updated last_row_count: {last_row_count}")
            elif current_row_count < last_row_count:
                log_message(f"⚠️ Rows deleted. Resetting last_row_count to {current_row_count}")
                last_row_count = current_row_count
            else:
                log_message("⏳ No new data.")
            await asyncio.sleep(POLLING_INTERVAL)
        except Exception as e:
            log_message(f"❌ Error in monitor_sheet: {e}")
            await asyncio.sleep(POLLING_INTERVAL)

# --- Force Scan Command ---
async def force_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global headers
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open(SPREADSHEET_NAME).worksheet(WORKSHEET_NAME)

        if headers is None:
            headers = await fetch_with_backoff(sheet.row_values, 1)

        all_rows = await fetch_with_backoff(sheet.get_all_values)
        last_3 = all_rows[-3:] if len(all_rows) > 3 else all_rows[1:]  # Skip headers
        for row in last_3:
            msg_md = "\n".join(
                f"*{escape_markdown_v2(headers[i] if i < len(headers) else f'Column {i+1}')}:* {escape_markdown_v2(cell)}"
                for i, cell in enumerate(row)
            )
            for chat_id in CHAT_IDS:
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"*Recent Entry:*\n{msg_md}",
                        parse_mode="MarkdownV2"
                    )
                except Exception as e:
                    if "bot can't initiate conversation" in str(e):
                        log_message(f"Cannot send message to chat_id {chat_id}: User has not initiated conversation with the bot.")
                    elif "bot was blocked" in str(e):
                        log_message(f"Cannot send message to chat_id {chat_id}: Bot was blocked by the user.")
                    else:
                        log_message(f"MarkdownV2 failed for row {row} (chat_id {chat_id}): {e}")
                        msg_html = "\n".join(
                            f"<b>{escape_html(headers[i] if i < len(headers) else f'Column {i+1}')}</b>: {escape_html(cell)}"
                            for i, cell in enumerate(row)
                        )
                        try:
                            await context.bot.send_message(
                                chat_id=chat_id,
                                text=f"<b>Recent Entry:</b>\n{msg_html}",
                                parse_mode="HTML"
                            )
                        except Exception as send_err:
                            log_message(f"HTML fallback failed for chat_id {chat_id}: {send_err}")
    except Exception as e:
        for chat_id in CHAT_IDS:
            try:
                await context.bot.send_message(chat_id=chat_id, text=f"Force scan error: {str(e)}")
            except Exception as send_err:
                log_message(f"Cannot send error message to chat_id {chat_id}: {send_err}")

# --- /start Command ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("Start Bot", callback_data='start_bot')],
        [InlineKeyboardButton("Stop Bot", callback_data='stop_bot')],
        [InlineKeyboardButton("Force Scan", callback_data='force_scan')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Control the bot below:", reply_markup=reply_markup)

# --- Button Handler ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global bot_running
    query = update.callback_query
    await query.answer()

    if query.data == "start_bot":
        if not bot_running:
            bot_running = True
            await query.edit_message_text("✅ Bot Started.")
            asyncio.create_task(monitor_sheet(context))
        else:
            await query.edit_message_text("ℹ️ Bot is already running.")
    elif query.data == "stop_bot":
        bot_running = False
        await query.edit_message_text("🛑 Bot Stopped.")
    elif query.data == "force_scan":
        await force_scan(update, context)

# --- Main Function ---
def main():
    # Initialize headers at startup to minimize API calls
    initialize_headers()

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))

    log_message("🤖 Bot is running. Press Ctrl+C to stop.")
    app.run_polling()

if __name__ == "__main__":
    main()