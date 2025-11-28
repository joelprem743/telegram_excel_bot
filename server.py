import os
from bot import application

if __name__ == "__main__":
    PORT = int(os.getenv("PORT", 10000))
    EXTERNAL = os.getenv("RENDER_EXTERNAL_URL")

    if not EXTERNAL:
        raise RuntimeError("RENDER_EXTERNAL_URL not set")

    # Normalize: remove https:// if user included it
    EXTERNAL = EXTERNAL.replace("https://", "").replace("http://", "")

    WEBHOOK_URL = f"https://{EXTERNAL}/webhook"

    print("===========================================")
    print("Starting Telegram bot via webhook mode")
    print("Webhook URL:", WEBHOOK_URL)
    print("Listening on port:", PORT)
    print("===========================================")

    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        webhook_url=WEBHOOK_URL,
        drop_pending_updates=True,
    )
