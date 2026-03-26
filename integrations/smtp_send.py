"""Отправка писем через SMTP (ваш ящик — Яндекс, Mail.ru, Gmail app password, корп. сервер)."""

from __future__ import annotations

import os
import re
import smtplib
import time
from email.message import EmailMessage
from email.utils import formataddr
from typing import Any, Optional

_MAX_RECIPIENTS = 180
_MAX_ATTACHMENT_BYTES = 12 * 1024 * 1024
_PAUSE_SEC = 0.08


def _truthy(val: Optional[str]) -> bool:
    if not val:
        return False
    return val.strip().lower() in ("1", "true", "yes", "on")


def smtp_settings_from_env() -> dict[str, Any]:
    host = (os.getenv("SMTP_HOST") or "").strip()
    user = (os.getenv("SMTP_USER") or "").strip()
    password = (os.getenv("SMTP_PASSWORD") or "").strip()
    from_addr = (os.getenv("SMTP_FROM") or "").strip() or user
    from_name = (os.getenv("SMTP_FROM_NAME") or "").strip()
    port_raw = (os.getenv("SMTP_PORT") or "").strip()
    port = int(port_raw) if port_raw.isdigit() else (465 if _truthy(os.getenv("SMTP_SSL")) else 587)
    use_ssl = _truthy(os.getenv("SMTP_SSL")) or port == 465
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "from_addr": from_addr,
        "from_name": from_name,
        "use_ssl": use_ssl,
    }


def smtp_configured() -> bool:
    s = smtp_settings_from_env()
    return bool(s["host"] and s["user"] and s["password"] and s["from_addr"])


def parse_recipients(raw: str) -> list[str]:
    if not raw or not raw.strip():
        return []
    parts = re.split(r"[\s,;]+", raw.strip())
    seen: set[str] = set()
    out: list[str] = []
    for p in parts:
        e = p.strip().lower()
        if not e or "@" not in e:
            continue
        if e not in seen:
            seen.add(e)
            out.append(e)
    return out


def send_bulk_plain(
    *,
    recipients: list[str],
    subject: str,
    body: str,
    attachment_name: Optional[str] = None,
    attachment_bytes: Optional[bytes] = None,
    attachment_content_type: Optional[str] = None,
) -> dict[str, Any]:
    if not smtp_configured():
        return {"ok": False, "error": "SMTP не настроен: задайте SMTP_HOST, SMTP_USER, SMTP_PASSWORD в .env"}
    if not recipients:
        return {"ok": False, "error": "Нет получателей"}
    if len(recipients) > _MAX_RECIPIENTS:
        return {"ok": False, "error": f"Слишком много адресов (макс. {_MAX_RECIPIENTS})"}
    subj = (subject or "").strip()
    if not subj:
        return {"ok": False, "error": "Пустая тема"}
    text = body or ""
    if attachment_bytes is not None and len(attachment_bytes) > _MAX_ATTACHMENT_BYTES:
        return {"ok": False, "error": f"Вложение больше {_MAX_ATTACHMENT_BYTES // (1024 * 1024)} МБ"}

    cfg = smtp_settings_from_env()
    host = cfg["host"]
    port = int(cfg["port"])
    user = cfg["user"]
    password = cfg["password"]
    from_addr = cfg["from_addr"]
    from_name = cfg["from_name"]

    sent = 0
    failed: list[dict[str, str]] = []

    for to in recipients:
        msg = EmailMessage()
        msg["Subject"] = subj
        if from_name:
            msg["From"] = formataddr((from_name, from_addr))
        else:
            msg["From"] = from_addr
        msg["To"] = to
        msg.set_content(text)
        if attachment_bytes and attachment_name:
            maintype = "application"
            subtype = "octet-stream"
            if attachment_content_type and "/" in attachment_content_type:
                maintype, _, subtype = attachment_content_type.partition("/")
            msg.add_attachment(
                attachment_bytes,
                maintype=maintype,
                subtype=subtype,
                filename=attachment_name,
            )
        try:
            if cfg["use_ssl"]:
                with smtplib.SMTP_SSL(host, port, timeout=60) as smtp:
                    smtp.login(user, password)
                    smtp.send_message(msg)
            else:
                with smtplib.SMTP(host, port, timeout=60) as smtp:
                    smtp.starttls()
                    smtp.login(user, password)
                    smtp.send_message(msg)
            sent += 1
            time.sleep(_PAUSE_SEC)
        except Exception as e:
            failed.append({"email": to, "error": str(e)})

    return {
        "ok": sent > 0 and len(failed) == 0,
        "sent": sent,
        "failed": failed,
        "total": len(recipients),
    }
