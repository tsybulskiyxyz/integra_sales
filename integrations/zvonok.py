"""Zvonok (ai.zvonok.com) — загрузка номеров в обзвон.

Точный URL и формат тела — возьмите в личном кабинете Zvonok (раздел API).
Подстройте ZVONOK_PHONES_ENDPOINT и тело запроса под актуальную документацию.
"""
import os
from typing import Any, Optional

import httpx


def push_phones(
    phones: list[str],
    campaign_id: Optional[str] = None,
    extra_payload: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Отправить список телефонов (только цифры 10+) в кампанию обзвона."""
    key = (os.getenv("ZVONOK_API_KEY") or "").strip()
    cid = (campaign_id or os.getenv("ZVONOK_CAMPAIGN_ID") or "").strip()
    base = (os.getenv("ZVONOK_API_URL") or "https://api.zvonok.com").rstrip("/")
    endpoint = (os.getenv("ZVONOK_PHONES_ENDPOINT") or "/v2/phones/tasks").strip()
    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint
    if not key:
        return {"ok": False, "error": "ZVONOK_API_KEY не задан в .env"}
    clean = []
    for p in phones:
        d = "".join(c for c in str(p) if c.isdigit())
        if len(d) >= 10:
            clean.append(d)
    if not clean:
        return {"ok": False, "error": "Нет валидных телефонов (мин. 10 цифр)"}
    payload: dict[str, Any] = {
        "public_key": key,
        "campaign_id": cid,
        "phones": clean,
    }
    if extra_payload:
        payload.update(extra_payload)
    try:
        use_json = (os.getenv("ZVONOK_USE_JSON", "1").strip() not in ("0", "false", "no"))
        with httpx.Client(timeout=120.0) as client:
            if use_json:
                r = client.post(f"{base}{endpoint}", json=payload)
            else:
                r = client.post(f"{base}{endpoint}", data=payload)
            body_preview = (r.text or "")[:4000]
            ok = r.status_code < 400
            return {
                "ok": ok,
                "http_status": r.status_code,
                "phones_sent": len(clean),
                "body_preview": body_preview,
            }
    except Exception as e:
        return {"ok": False, "error": str(e)}
