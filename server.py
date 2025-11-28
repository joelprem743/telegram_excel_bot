import os
import asyncio
from aiohttp import web
from dotenv import load_dotenv
from bot import application   # PTB Application instance

load_dotenv()
TOKEN = os.getenv("TOKEN")

# Telegram sends POST updates here
async def handle_webhook(request):
    try:
        data = await request.json()
        await application.update_queue.put(data)
    except Exception as e:
        print("Webhook error:", e)
    return web.Response(text="OK")


async def start():
    # Render provides external hostname
    hostname = os.getenv("RENDER_EXTERNAL_HOSTNAME")
    if not hostname:
        raise RuntimeError("RENDER_EXTERNAL_HOSTNAME missing. Render must provide it.")

    webhook_url = f"https://{hostname}/webhook/{TOKEN}"

    # Remove old webhook
    await application.bot.delete_webhook(drop_pending_updates=True)

    # Set new webhook
    await application.bot.set_webhook(url=webhook_url)

    # Create aiohttp app for webhook handling
    app = web.Application()
    app.add_routes([web.post(f"/webhook/{TOKEN}", handle_webhook)])

    # Bind to Render PORT
    port = int(os.getenv("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    print("=========================================")
    print("Webhook set to:", webhook_url)
    print("Listening on port:", port)
    print("=========================================")

    # Start PTB application (NO POLLING)
    await application.initialize()
    await application.start()
    await application.stop()  # never reached but required by PTB


if __name__ == "__main__":
    asyncio.run(start())
