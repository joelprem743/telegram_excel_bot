import os
from flask import Flask, request
from bot import application  # import telegram application

TOKEN = os.getenv("TOKEN")

app = Flask(__name__)

@app.post(f"/webhook/{TOKEN}")
def webhook() -> str:
    """Receives Telegram updates from webhook."""
    json_data = request.get_json(force=True, silent=True)
    if json_data:
        application.update_queue.put(json_data)
    return "OK", 200

@app.get("/")
def home():
    return "Telegram Excel Bot Webhook Server Running", 200
