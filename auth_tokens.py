"""Хранение токенов для magic-link входа."""
from datetime import datetime, timedelta
import secrets


_tokens: dict[str, dict] = {}


def create(contact: dict, telegram_id: str) -> str:
    """Создать токен входа. Возвращает token."""
    token = secrets.token_urlsafe(32)
    _tokens[token] = {
        "contact": contact,
        "telegram_id": telegram_id,
        "expires": datetime.now() + timedelta(minutes=10),
    }
    return token


def consume(token: str) -> dict | None:
    """Проверить и забрать токен. Возвращает данные или None."""
    data = _tokens.pop(token, None)
    if not data or data["expires"] < datetime.now():
        return None
    return data
