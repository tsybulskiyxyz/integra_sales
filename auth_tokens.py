"""Хранение токенов для magic-link входа (в БД — переживает перезапуск и несколько воркеров)."""
from datetime import datetime, timedelta
import secrets

from database import save_auth_token, consume_auth_token as _db_consume


def create(contact: dict, telegram_id: str) -> str:
    """Создать токен входа. Возвращает token."""
    token = secrets.token_urlsafe(32)
    expires_at = (datetime.now() + timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
    save_auth_token(
        token=token,
        contact_id=contact["id"],
        name=contact["name"],
        role=contact["role"],
        telegram_id=telegram_id,
        expires_at=expires_at,
    )
    return token


def consume(token: str) -> dict | None:
    """Проверить и забрать токен. Возвращает данные или None."""
    return _db_consume(token)
