"""Конфигурация приложения."""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Google Sheets
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials/service_account.json")
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL", "")

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Database
BASE_DIR = Path(__file__).resolve().parent
DATABASE_PATH = os.getenv("DATABASE_PATH", str(BASE_DIR / "integra.db"))

# Session
SESSION_SECRET = os.getenv("SESSION_SECRET", "integra-secret-key-change-me")

# App
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://localhost:8000")
# Ссылка для входа в Telegram (если задана — используется вместо APP_BASE_URL).
# Для кликабельной кнопки нужен публичный URL (https). Локально: ngrok → LOGIN_LINK_URL=https://xxx.ngrok-free.app
LOGIN_LINK_URL = os.getenv("LOGIN_LINK_URL", "").strip() or None
