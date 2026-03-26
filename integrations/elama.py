"""eLama — выгрузка контактов для ретаргетинга / аудиторий.

Актуальный метод и заголовки авторизации — в кабинете eLama (API / справка).
Подстройте ELAMA_CONTACTS_ENDPOINT и формат тела под ваш договор.
"""
import os
from typing import Any, Optional

import httpx


def push_emails(
    emails: list[str],
    audience_id: Optional[str] = None,
    extra_payload: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    token = (os.getenv("ELAMA_ACCESS_TOKEN") or "").strip()
    aud = (audience_id or os.getenv("ELAMA_AUDIENCE_ID") or "").strip()
    base = (os.getenv("ELAMA_API_BASE") or "https://api.elama.global").rstrip("/")
    endpoint = (os.getenv("ELAMA_CONTACTS_ENDPOINT") or "/v1/audience/contacts").strip()
    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint
    if not token:
        return {"ok": False, "error": "ELAMA_ACCESS_TOKEN не задан в .env"}
    clean: list[str] = []
    seen: set[str] = set()
    for e in emails:
        em = (e or "").strip().lower()
        if "@" in em and em not in seen:
            seen.add(em)
            clean.append(em)
    if not clean:
        return {"ok": False, "error": "Нет валидных email"}
    payload: dict[str, Any] = {"audience_id": aud, "emails": clean}
    if extra_payload:
        payload.update(extra_payload)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    auth_header = os.getenv("ELAMA_AUTH_HEADER", "").strip()
    if auth_header:
        headers["Authorization"] = auth_header
    try:
        with httpx.Client(timeout=120.0) as client:
            r = client.post(f"{base}{endpoint}", json=payload, headers=headers)
            body_preview = (r.text or "")[:4000]
            ok = r.status_code < 400
            return {
                "ok": ok,
                "http_status": r.status_code,
                "emails_sent": len(clean),
                "body_preview": body_preview,
            }
    except Exception as e:
        return {"ok": False, "error": str(e)}
