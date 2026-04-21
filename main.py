"""Точка входа — FastAPI + веб-интерфейс."""
import asyncio
import csv
import io
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime as _dt, timedelta
from typing import Optional

from fastapi import FastAPI, Request, HTTPException, File, Form, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware
from pydantic import BaseModel
import uvicorn

from config import GOOGLE_SHEET_URL, GOOGLE_LEGAL_SHEET_URL, SESSION_SECRET, APP_BASE_URL, TELEGRAM_CHAT_ID
from database import (
    init_db,
    add_comment,
    add_reminder,
    get_pending_reminders,
    mark_reminder_sent,
    add_contact,
    get_contacts_by_role,
    get_all_contacts,
    delete_contact,
    set_econom_number,
    set_econom_number_by_id,
    set_local_status,
    set_local_status_by_id,
    set_object_info,
    set_object_info_by_id,
    get_row_extra_by_id,
    get_row_extra_id_by_phone_row,
    get_row_extras,
    resolve_row_extra,
    get_last_activity_by_row,
    resolve_last_activity,
    normalize_crm_phone,
    add_event,
    get_events,
    delete_event,
    get_inactive_clients,
    get_max_stages,
    save_task_message,
    get_todays_reminders,
    get_overdue_reminders,
    get_status_summary,
    get_tasks_for_user,
    get_all_tasks_for_manager,
    update_task_status,
    get_task_by_id,
    delete_task,
    get_unfinished_tasks_for_reminder,
    record_task_reminder_sent,
    LEGAL_LEAD_STATUSES,
    legal_leads_list,
    legal_lead_summary,
    legal_lead_create,
    legal_lead_update,
    legal_lead_add_event,
    legal_lead_events,
    legal_import_upsert_row,
    legal_lead_get,
    legal_dashboard_next_contact_buckets,
    legal_inactive_for_dashboard,
)
from google_sheets import (
    fetch_call_data,
    get_spreadsheet_last_modified,
    fetch_legal_sheet_rows,
    fetch_legal_sheet_dashboard_rows,
    fetch_sheet_flat_text,
)
from integrations.smtp_send import send_bulk_plain, parse_recipients, smtp_configured, smtp_settings_from_env
from integrations.email_harvest import extract_emails_from_text, extract_emails_from_upload
from stats import calculate_stats
from telegram_bot import send_reminder, send_task_to_role, send_task_reminder, send_telegram, send_document, send_photo, add_task_status_keyboard, send_task_status_to_recipient, send_task_from_worker, send_weekly_report, set_bot_commands
from models import RowStatus


async def _reminder_loop():
    """Фоновая проверка напоминаний каждые 10 секунд."""
    while True:
        try:
            pending = get_pending_reminders()
            extras = get_row_extras()
            for r in pending:
                chat_id = r.get("recipient") or None
                key = (r["phone"], r["sheet_row"])
                extra = extras.get(key, {})
                client_name = extra.get("econom_number", "")
                local_status = extra.get("local_status", "")
                events = get_events(r["phone"], limit=5)
                if send_reminder(
                    r["phone"], r["text"], r["sheet_row"], chat_id,
                    client_name=client_name,
                    local_status=local_status,
                    events=events,
                ):
                    mark_reminder_sent(r["id"])
                    add_event(r["phone"], "reminder_sent", f"Напоминание отправлено: {r['text']}", r["sheet_row"])
        except Exception:
            pass
        await asyncio.sleep(10)


async def _task_reminder_loop():
    """Напоминания сотрудникам о незавершённых задачах — раз в 6 ч, не чаще раза в 24ч на задачу."""
    await asyncio.sleep(300)  # первая проверка через 5 мин после старта
    while True:
        try:
            tasks = get_unfinished_tasks_for_reminder(hours_since_last=24)
            seen_chats = set()
            for t in tasks:
                if t["tg_chat_id"] in seen_chats:
                    continue
                seen_chats.add(t["tg_chat_id"])
                if send_task_reminder(
                    t["tg_chat_id"],
                    t["task_text"],
                    t["phone"],
                    t.get("client_name", ""),
                ):
                    record_task_reminder_sent(t["id"])
        except Exception:
            pass
        await asyncio.sleep(6 * 3600)  # каждые 6 часов


def _build_weekly_report() -> Optional[str]:
    """Отчёт по звонкам: последние 5 рабочих дней (по блокам между 'Call id'). Даты — из времени изменения таблицы."""
    if not GOOGLE_SHEET_URL:
        return None
    try:
        rows, working_days = fetch_call_data()
        last_modified = get_spreadsheet_last_modified()
        if not rows:
            return "За неделю звонков не зафиксировано. Таблица пуста."
        max_day = max(r.day_index for r in rows)
        # Последние 5 рабочих дней (блоки между заголовками "Call id")
        week_rows = [r for r in rows if r.day_index >= max(0, max_day - 4)]
        now = _dt.now()
        days_since_monday = now.weekday()
        monday_start = (now - timedelta(days=days_since_monday)).strftime("%d.%m.%Y")
        date_to = now.strftime("%d.%m.%Y")
        lines = [
            f"Период: с понедельника {monday_start} — {date_to}",
            f"Данные таблицы: {last_modified or '—'}",
            "",
        ]
        stats = calculate_stats(week_rows, working_days)
        lines.extend([
            f"Всего звонков: {stats.total_rows}",
            f"Дозвоны (красный+зелёный+фиолетовый): {stats.reached_count}",
            f"Целевые (зелёные): {stats.green_count}",
            f"Закрытые сделки (фиолетовые): {stats.purple_count}",
            f"Не дозвонились (оранжевые): {stats.orange_count}",
            f"% целевых от дозвонов: {stats.target_percent}",
            f"% закрытых от целевых: {stats.closed_percent}",
        ])
        # Из прозвоненных за неделю ушли на КП
        extras = get_row_extras()
        max_stages = get_max_stages()
        reached_week = [r for r in week_rows if r.status in (RowStatus.RED, RowStatus.GREEN, RowStatus.PURPLE)]
        kp_count = 0
        for r in reached_week:
            key = (normalize_crm_phone(r.phone), int(r.row_index))
            local_status = resolve_row_extra(extras, r.phone, r.row_index).get("local_status", "first_contact")
            stage_rank = max_stages.get(key, 0)
            if local_status in ("proposal_sent", "closed") or stage_rank >= 3:
                kp_count += 1
        lines.append(f"Из прозвоненных за неделю ушли на КП: {kp_count} из {len(reached_week)}")
        return "\n".join(lines)
    except Exception:
        return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    set_bot_commands()
    t1 = asyncio.create_task(_reminder_loop())
    t4 = asyncio.create_task(_task_reminder_loop())
    from bot import run_polling
    t3 = asyncio.create_task(run_polling())
    yield
    t1.cancel()
    t3.cancel()
    t4.cancel()


app = FastAPI(title="Integra Sales", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

ROLE_LABELS = {
    "sales_manager": "Менеджер по продажам",
    "estimator": "Сметчица",
    "engineer": "Инженер",
    "sales_head": "Руководитель ОП",
    "test": "Тест",
}

MANAGER_ROLES = {"sales_manager", "sales_head"}


def _get_user(request: Request) -> Optional[dict]:
    return request.session.get("user")


def _require_user(request: Request) -> dict:
    user = _get_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return user


_AUTH_PUBLIC = ("/login", "/logout", "/docs", "/openapi.json", "/favicon.ico")


class _AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path in _AUTH_PUBLIC or path.startswith("/auth/"):
            return await call_next(request)
        if path.startswith("/api/") and not _get_user(request):
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)
        return await call_next(request)


app.add_middleware(_AuthMiddleware)
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)


class CommentInput(BaseModel):
    phone: str
    comment: str
    sheet_row: Optional[int] = None


class ReminderInput(BaseModel):
    phone: str
    text: str
    reminder_at: str
    sheet_row: Optional[int] = None
    recipient_telegram_id: Optional[str] = None


class SendNowInput(BaseModel):
    phone: str
    text: str
    sheet_row: Optional[int] = None
    recipient_telegram_id: Optional[str] = None


class ContactInput(BaseModel):
    name: str
    role: str
    telegram_id: Optional[str] = None


class TaskInput(BaseModel):
    phone: str
    object_info: str
    task: str
    role: str
    recipient_telegram_id: Optional[str] = None


class EconomNumberInput(BaseModel):
    phone: str = ""
    sheet_row: int = 0
    econom_number: Optional[str] = ""
    extra_id: Optional[int] = None


class LocalStatusInput(BaseModel):
    phone: str = ""
    sheet_row: int = 0
    local_status: str
    extra_id: Optional[int] = None


class ObjectInfoInput(BaseModel):
    phone: str = ""
    sheet_row: int = 0
    address: str = ""
    area: str = ""
    budget: str = ""
    work_type: str = ""
    extra_id: Optional[int] = None


class LegalLeadCreateInput(BaseModel):
    company_name: str
    inn: Optional[str] = ""
    phone: Optional[str] = ""
    email: Optional[str] = ""
    okved: Optional[str] = ""
    region: Optional[str] = ""
    source: Optional[str] = "manual"
    next_contact_at: Optional[str] = ""
    priority: Optional[int] = 0


class LegalLeadPatchInput(BaseModel):
    company_name: Optional[str] = None
    inn: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    okved: Optional[str] = None
    region: Optional[str] = None
    status: Optional[str] = None
    notes: Optional[str] = None
    next_contact_at: Optional[str] = None
    priority: Optional[int] = None


class LegalLeadEventInput(BaseModel):
    description: str


LEGAL_LEAD_STATUS_LABELS = {
    "first_contact": "Первый контакт",
    "negotiation": "Переговоры",
    "object_quote": "Объект в просчёте",
    "object_work": "Ведутся работы на объекте",
    "closed": "Закрыт",
}


@app.get("/favicon.ico")
async def favicon():
    svg = (
        b'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 28 28">'
        b'<rect x="10" y="4" width="8" height="20" rx="2" fill="#fff"/>'
        b'<rect x="12" y="8" width="4" height="3" rx="1" fill="#000"/>'
        b'</svg>'
    )
    return Response(content=svg, media_type="image/svg+xml")


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if _get_user(request):
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.get("/auth/{token}")
async def auth_by_token(request: Request, token: str):
    from auth_tokens import consume
    pending = consume(token)
    if not pending:
        return templates.TemplateResponse("login.html", {
            "request": request, "error": "Ссылка истекла. Отправьте /start боту ещё раз.",
        })
    contact = pending["contact"]
    request.session["user"] = {
        "id": contact["id"],
        "name": contact["name"],
        "role": contact["role"],
        "role_label": ROLE_LABELS.get(contact["role"], contact["role"]),
        "telegram_id": pending["telegram_id"],
    }
    return RedirectResponse("/", status_code=302)


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=302)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    user = _get_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)
    is_manager = user.get("role") in MANAGER_ROLES
    return templates.TemplateResponse("index.html", {
        "request": request, "user": user, "is_manager": is_manager,
    })


@app.get("/api/dashboard")
async def get_dashboard():
    """Данные для дашборда 'Мой день'."""
    if not GOOGLE_SHEET_URL:
        raise HTTPException(400, "GOOGLE_SHEET_URL не задан в .env")
    rows, _ = fetch_call_data()
    orange = [{"phone": r.phone, "creation_time": r.creation_time, "row_index": r.row_index}
              for r in rows if r.status == RowStatus.ORANGE]
    today_rem = get_todays_reminders()
    overdue = get_overdue_reminders()
    inactive = get_inactive_clients(days=3)
    extras = get_row_extras()
    for item in today_rem + overdue:
        sr = item.get("sheet_row")
        extra = resolve_row_extra(extras, item.get("phone") or "", sr) if sr is not None else {}
        item["client_name"] = extra.get("econom_number", "")
    for item in inactive:
        item["days_silent"] = ""
        if item.get("last_activity"):
            try:
                last = _dt.strptime(item["last_activity"], "%Y-%m-%d %H:%M:%S")
                item["days_silent"] = (_dt.now() - last).days
            except Exception:
                pass
    summary = get_status_summary()
    return {
        "orange": orange,
        "today_reminders": today_rem,
        "overdue": overdue,
        "inactive": inactive[:15],
        "summary": summary,
    }


@app.get("/api/data")
async def get_data():
    """Загрузка данных из Google Sheets."""
    if not GOOGLE_SHEET_URL:
        raise HTTPException(400, "GOOGLE_SHEET_URL не задан в .env")
    rows, working_days = fetch_call_data()
    extras = get_row_extras()
    max_stages = get_max_stages()
    last_activity_map = get_last_activity_by_row()
    stage_names = {0: "first_contact", 1: "negotiation", 2: "waiting", 3: "proposal_sent", 4: "closed"}
    orange_rows = [{"row_index": r.row_index, "phone": r.phone, "creation_time": r.creation_time}
                   for r in rows if r.status == RowStatus.ORANGE]
    out_rows = []
    for r in rows:
        if r.status == RowStatus.ORANGE:
            continue
        extra = resolve_row_extra(extras, r.phone, r.row_index)
        if not extra and r.status != RowStatus.GREEN:
            continue
        if not extra:
            extra = {}
        local_status = extra.get("local_status", "first_contact")
        max_rank = max_stages.get((normalize_crm_phone(r.phone), int(r.row_index)), 0)
        current_rank = {"first_contact": 0, "negotiation": 1, "waiting": 2, "proposal_sent": 3, "closed": 4}.get(local_status, -1)
        best_rank = max(max_rank, current_rank) if current_rank >= 0 else max_rank
        max_stage = "contractors" if local_status == "contractors" else stage_names.get(best_rank, "first_contact")
        out_rows.append({
            "row_index": r.row_index,
            "phone": r.phone,
            "status": r.status.value,
            "creation_time": r.creation_time,
            "last_activity": resolve_last_activity(last_activity_map, r.phone, r.row_index),
            "extra_id": extra.get("id"),
            "econom_number": extra.get("econom_number", ""),
            "local_status": local_status,
            "max_stage": max_stage,
            "object_address": extra.get("object_address", ""),
            "object_area": extra.get("object_area", ""),
            "object_budget": extra.get("object_budget", ""),
            "object_work_type": extra.get("object_work_type", ""),
        })
    stats = calculate_stats(rows, working_days)
    total_calls = len(rows)
    avg_per_day = round(total_calls / working_days, 1) if working_days else 0
    summary = get_status_summary()
    # Воронка: текущий статус, но proposal_sent дополняем отказавшими после КП
    funnel_summary = dict(summary)
    funnel_summary.setdefault("first_contact", 0)
    funnel_summary.setdefault("negotiation", 0)
    funnel_summary.setdefault("waiting", 0)
    funnel_summary.setdefault("proposal_sent", 0)
    funnel_summary.setdefault("closed", 0)
    rejected_after_kp = sum(1 for row in out_rows if row.get("local_status") == "rejected" and row.get("max_stage") == "proposal_sent")
    funnel_summary["proposal_sent"] = (funnel_summary.get("proposal_sent") or 0) + rejected_after_kp
    funnel_summary["_rejected_after_kp"] = rejected_after_kp
    return {
        "rows": out_rows,
        "orange_rows": orange_rows,
        "summary": summary,
        "funnel_summary": funnel_summary,
        "stats": {
            "total_rows": stats.total_rows,
            "red_count": stats.red_count,
            "green_count": stats.green_count,
            "orange_count": stats.orange_count,
            "purple_count": stats.purple_count,
            "reached_count": stats.reached_count,
            "target_percent": stats.target_percent,
            "closed_percent": stats.closed_percent,
            "working_days": stats.working_days,
            "avg_per_day": avg_per_day,
        },
    }


def _legal_normalize_phone_keys(phone: str) -> set[str]:
    """Последние 10 цифр каждого номера в строке (как при сопоставлении с листом и legal_leads)."""
    out: set[str] = set()
    for part in re.split(r"[,;/\s]+", phone or ""):
        d = re.sub(r"\D", "", part)
        if len(d) >= 10:
            out.add(d[-10:])
    return out


# Ключи зелёных строк листа (после sync свежие; иначе TTL — второй запрос к Sheets).
_LEGAL_GREEN_SHEET_CACHE: dict = {"url": "", "at": 0.0, "keys": None}
_LEGAL_GREEN_CACHE_TTL_SEC = 90.0


def _legal_green_keys_from_import_rows(rows: list[dict]) -> set[str]:
    keys: set[str] = set()
    for d in rows:
        inn = (d.get("inn") or "").strip()
        if inn:
            keys.add(f"inn:{inn}")
        for k in _legal_normalize_phone_keys(d.get("phone") or ""):
            keys.add(f"ph:{k}")
    return keys


def _legal_lead_matches_green_keys(lead: dict, keys: set[str]) -> bool:
    inn = (lead.get("inn") or "").strip()
    if inn and f"inn:{inn}" in keys:
        return True
    for k in _legal_normalize_phone_keys(lead.get("phone") or ""):
        if f"ph:{k}" in keys:
            return True
    return False


def _legal_refresh_green_sheet_cache(url: str, pack: dict) -> set[str]:
    rows = pack.get("rows") or []
    k = _legal_green_keys_from_import_rows(rows)
    _LEGAL_GREEN_SHEET_CACHE["url"] = url
    _LEGAL_GREEN_SHEET_CACHE["at"] = time.monotonic()
    _LEGAL_GREEN_SHEET_CACHE["keys"] = frozenset(k)
    return k


def _legal_get_green_keys_for_url(url: str) -> set[str]:
    if not url:
        return set()
    c = _LEGAL_GREEN_SHEET_CACHE
    if (
        c["url"] == url
        and c["keys"] is not None
        and (time.monotonic() - float(c["at"])) < _LEGAL_GREEN_CACHE_TTL_SEC
    ):
        return set(c["keys"])
    pack = fetch_legal_sheet_rows(url)
    return _legal_refresh_green_sheet_cache(url, pack)


def _legal_filter_first_contact_by_green_sheet(leads: list[dict], sheet_url: str) -> list[dict]:
    """Только лиды, которые сейчас есть среди зелёных строк юр-листа (как в таблице)."""
    if not sheet_url.strip():
        return leads
    keys = _legal_get_green_keys_for_url(sheet_url.strip())
    return [L for L in leads if _legal_lead_matches_green_keys(L, keys)]


def _legal_event_is_sheet_import_noise(ev: dict) -> bool:
    """Служебные записи синка с Google — не показываем в истории лида."""
    t = (ev.get("type") or "").strip()
    desc = (ev.get("description") or "").strip()
    if t == "note" and desc.startswith("Из таблицы:"):
        return True
    if t == "system" and (
        desc.startswith("Импорт (")
        or desc.startswith("Импорт:")
        or "Импорт: обновление" in desc
        or "источник google_sheet" in desc
        or "источник google_sheets" in desc
    ):
        return True
    return False


def _legal_lead_events_for_ui(lead_id: int) -> list[dict]:
    raw = legal_lead_events(lead_id, limit=120)
    out = [e for e in raw if not _legal_event_is_sheet_import_noise(e)]
    return out[:50]


def _legal_orange_rows_enriched(sheet: dict) -> list[dict]:
    """Оранжевые строки листа с привязкой к lead_id в CRM (ИНН или телефон)."""
    leads = legal_leads_list()
    phone_to_id: dict[str, int] = {}
    inn_to_id: dict[str, int] = {}
    for L in leads:
        inn = (L.get("inn") or "").strip()
        if inn:
            inn_to_id[inn] = int(L["id"])
        for key in _legal_normalize_phone_keys(L.get("phone") or ""):
            phone_to_id[key] = int(L["id"])

    orange_out = []
    for o in sheet.get("orange") or []:
        lead_id = None
        sr = o.get("row_index")
        inn = ""
        for pr in sheet.get("rows") or []:
            if pr.get("sheet_row") == sr:
                inn = (pr.get("inn") or "").strip()
                break
        if inn and inn in inn_to_id:
            lead_id = inn_to_id[inn]
        else:
            raw = (o.get("phone") or "").split(",")[0]
            dig = re.sub(r"\D", "", raw)
            if len(dig) >= 10:
                lead_id = phone_to_id.get(dig[-10:])
        row = dict(o)
        row["lead_id"] = lead_id
        orange_out.append(row)
    return orange_out


@app.get("/api/legal/dashboard")
async def api_legal_dashboard(request: Request):
    """Дашборд юриков: те же блоки, что у физиков; «Дозвонить» и сводка по цветам — из GOOGLE_LEGAL_SHEET_URL."""
    user = _require_user(request)
    if user.get("role") not in MANAGER_ROLES:
        raise HTTPException(403, "Только менеджер")
    if not GOOGLE_LEGAL_SHEET_URL:
        raise HTTPException(400, "GOOGLE_LEGAL_SHEET_URL не задан в .env")
    try:
        sheet = fetch_legal_sheet_dashboard_rows(GOOGLE_LEGAL_SHEET_URL)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        raise HTTPException(502, str(e))

    orange_out = _legal_orange_rows_enriched(sheet)

    overdue, today_r = legal_dashboard_next_contact_buckets()
    inactive = legal_inactive_for_dashboard()
    sm = legal_lead_summary()
    cs = sheet["color_summary"]
    sheet_url = (GOOGLE_LEGAL_SHEET_URL or "").strip()
    fc_strip = sm.get("first_contact", 0)
    if sheet_url:
        fc_strip = len(
            _legal_filter_first_contact_by_green_sheet(legal_leads_list("first_contact"), sheet_url)
        )
    summary_strip = [
        {"label": "Всего в листе", "value": sheet["total_rows"], "color": "var(--text)"},
        {"label": "Зелёный", "value": cs.get("green", 0), "color": "var(--green)"},
        {"label": "Оранжевый", "value": cs.get("orange", 0), "color": "var(--accent)"},
        {"label": "Красный", "value": cs.get("red", 0), "color": "var(--red)"},
        {"label": "Фиолетовый", "value": cs.get("purple", 0), "color": "var(--purple)"},
        {"label": "Без цвета", "value": cs.get("unknown", 0), "color": "var(--muted)"},
        {"label": "Первый контакт", "value": fc_strip, "color": "var(--green)"},
        {"label": "Переговоры", "value": sm.get("negotiation", 0), "color": "var(--text)"},
        {"label": "В просчёте", "value": sm.get("object_quote", 0), "color": "var(--orange)"},
        {"label": "Работы на объекте", "value": sm.get("object_work", 0), "color": "var(--purple)"},
        {"label": "Закрыт", "value": sm.get("closed", 0), "color": "var(--muted)"},
    ]
    return {
        "overdue": overdue,
        "today_reminders": today_r,
        "orange": orange_out,
        "inactive": inactive,
        "summary_strip": summary_strip,
    }


@app.get("/api/legal/callback-sheet")
async def api_legal_callback_sheet(request: Request):
    """Оранжевые строки юрлист-таблицы («Дозвонить»), как у физиков — не из статуса CRM."""
    user = _require_user(request)
    if user.get("role") not in MANAGER_ROLES:
        raise HTTPException(403, "Только менеджер")
    if not GOOGLE_LEGAL_SHEET_URL:
        raise HTTPException(400, "GOOGLE_LEGAL_SHEET_URL не задан в .env")
    try:
        sheet = fetch_legal_sheet_dashboard_rows(GOOGLE_LEGAL_SHEET_URL)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        raise HTTPException(502, str(e))
    orange = _legal_orange_rows_enriched(sheet)
    return {"orange": orange, "count": len(orange)}


@app.get("/api/legal/summary")
async def api_legal_summary(request: Request):
    user = _require_user(request)
    if user.get("role") not in MANAGER_ROLES:
        raise HTTPException(403, "Только менеджер")
    return legal_lead_summary()


@app.get("/api/legal/leads")
async def api_legal_leads_list(request: Request, status: Optional[str] = None):
    user = _require_user(request)
    if user.get("role") not in MANAGER_ROLES:
        raise HTTPException(403, "Только менеджер")
    st = status if status in LEGAL_LEAD_STATUSES else None
    sheet_url = (GOOGLE_LEGAL_SHEET_URL or "").strip()
    sumy = dict(legal_lead_summary())
    if sheet_url:
        fc_on_green = _legal_filter_first_contact_by_green_sheet(
            legal_leads_list("first_contact"),
            sheet_url,
        )
        sumy["first_contact"] = len(fc_on_green)
        if st == "first_contact":
            leads = fc_on_green
        else:
            leads = legal_leads_list(st, due_only=False)
    else:
        leads = legal_leads_list(st, due_only=False)
    return {
        "leads": leads,
        "summary": sumy,
        "status_labels": LEGAL_LEAD_STATUS_LABELS,
        "status_filter_order": list(LEGAL_LEAD_STATUSES),
    }


@app.get("/api/legal/leads/{lead_id}")
async def api_legal_lead_detail(request: Request, lead_id: int):
    user = _require_user(request)
    if user.get("role") not in MANAGER_ROLES:
        raise HTTPException(403, "Только менеджер")
    row = legal_lead_get(lead_id)
    if not row:
        raise HTTPException(404, "Не найдено")
    return {**row, "events": _legal_lead_events_for_ui(lead_id), "status_labels": LEGAL_LEAD_STATUS_LABELS}


@app.post("/api/legal/leads")
async def api_legal_lead_create(request: Request, data: LegalLeadCreateInput):
    user = _require_user(request)
    if user.get("role") not in MANAGER_ROLES:
        raise HTTPException(403, "Только менеджер")
    if not data.company_name.strip() and not (data.phone or "").strip():
        raise HTTPException(400, "Укажите название компании или телефон")
    lead_id = legal_lead_create(
        data.company_name.strip(),
        data.inn or "",
        data.phone or "",
        data.email or "",
        data.okved or "",
        data.region or "",
        data.source or "manual",
        "first_contact",
        (data.next_contact_at or "").strip(),
        int(data.priority or 0),
    )
    legal_lead_add_event(lead_id, f"Создано менеджером {user.get('name', '')}", "system")
    return {"ok": True, "id": lead_id}


@app.patch("/api/legal/leads/{lead_id}")
async def api_legal_lead_patch(request: Request, lead_id: int, data: LegalLeadPatchInput):
    user = _require_user(request)
    if user.get("role") not in MANAGER_ROLES:
        raise HTTPException(403, "Только менеджер")
    row = legal_lead_get(lead_id)
    if not row:
        raise HTTPException(404, "Не найдено")
    dump = data.model_dump(exclude_unset=True)
    if "status" in dump and dump["status"] not in LEGAL_LEAD_STATUSES:
        raise HTTPException(400, "Неверный статус")
    old_status = row["status"]
    ok = legal_lead_update(
        lead_id,
        company_name=dump.get("company_name"),
        inn=dump.get("inn"),
        phone=dump.get("phone"),
        email=dump.get("email"),
        okved=dump.get("okved"),
        region=dump.get("region"),
        status=dump.get("status"),
        notes=dump.get("notes"),
        next_contact_at=dump.get("next_contact_at"),
        priority=dump.get("priority"),
    )
    if not ok:
        raise HTTPException(404, "Не найдено")
    if dump.get("status") and dump["status"] != old_status:
        legal_lead_add_event(
            lead_id,
            f"Статус: {LEGAL_LEAD_STATUS_LABELS.get(old_status, old_status)} → {LEGAL_LEAD_STATUS_LABELS.get(dump['status'], dump['status'])}",
            "status_change",
        )
    return {"ok": True}


@app.post("/api/legal/leads/{lead_id}/events")
async def api_legal_lead_add_event(request: Request, lead_id: int, data: LegalLeadEventInput):
    user = _require_user(request)
    if user.get("role") not in MANAGER_ROLES:
        raise HTTPException(403, "Только менеджер")
    if not data.description.strip():
        raise HTTPException(400, "Пустая заметка")
    if not legal_lead_add_event(lead_id, f"{user.get('name', '')}: {data.description.strip()}", "note"):
        raise HTTPException(404, "Лид не найден")
    return {"ok": True}


@app.post("/api/legal/sync")
async def api_legal_sync(request: Request):
    """Импорт строк из Google-таблицы юриков в CRM. URL — только GOOGLE_LEGAL_SHEET_URL в .env (без UI)."""
    user = _require_user(request)
    if user.get("role") not in MANAGER_ROLES:
        raise HTTPException(403, "Только менеджер")
    url = GOOGLE_LEGAL_SHEET_URL.strip()
    if not url:
        raise HTTPException(400, "В .env задайте GOOGLE_LEGAL_SHEET_URL (ссылка на таблицу юриков)")
    try:
        pack = fetch_legal_sheet_rows(url)
        rows = pack["rows"]
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    except Exception as e:
        raise HTTPException(502, f"Не удалось прочитать таблицу: {e}") from e
    created = updated = skipped = 0
    for d in rows:
        action, lid = legal_import_upsert_row(
            d.get("company_name") or "",
            d.get("inn") or "",
            d.get("phone") or "",
            d.get("email") or "",
            d.get("okved") or "",
            d.get("region") or "",
            "google_sheet",
            d.get("next_contact_at") or "",
            int(d.get("priority") or 0),
            crm_status=d.get("crm_status") or None,
        )
        if action == "created":
            created += 1
        elif action == "updated":
            updated += 1
        else:
            skipped += 1
    _legal_refresh_green_sheet_cache(url, pack)
    return {
        "ok": True,
        "row_count": len(rows),
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "skipped_by_color": pack.get("skipped_by_color", 0),
        "color_filter_applied": pack.get("color_filter_active", False),
        "color_read_failed": pack.get("color_read_failed", False),
        "orange_on_sheet": int(pack.get("orange_on_sheet") or 0),
    }


@app.get("/api/legal/export")
async def api_legal_export(request: Request, status: Optional[str] = None):
    user = _require_user(request)
    if user.get("role") not in MANAGER_ROLES:
        raise HTTPException(403, "Только менеджер")
    st = status if status in LEGAL_LEAD_STATUSES else None
    sheet_url = (GOOGLE_LEGAL_SHEET_URL or "").strip()
    leads = legal_leads_list(st)
    if st == "first_contact" and sheet_url:
        leads = _legal_filter_first_contact_by_green_sheet(leads, sheet_url)
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";")
    w.writerow(
        ["company_name", "inn", "phone", "email", "okved", "region", "status", "priority", "next_contact_at", "notes", "source", "updated_at"]
    )
    for r in leads:
        w.writerow(
            [
                r["company_name"],
                r["inn"],
                r["phone"],
                r["email"],
                r["okved"],
                r["region"],
                r["status"],
                r["priority"],
                r["next_contact_at"],
                re.sub(r"[\r\n]+", " ", r["notes"] or ""),
                r["source"],
                r["updated_at"] or "",
            ]
        )
    return Response(
        buf.getvalue().encode("utf-8-sig"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="integra_legal_leads.csv"'},
    )


@app.post("/api/mail/extract-emails")
async def api_mail_extract_emails(
    request: Request,
    file: Optional[UploadFile] = File(None),
    sheet_url: str = Form(""),
):
    """Достать email из PDF / Excel / CSV / TXT или из Google Таблицы (первая вкладка, A:AZ)."""
    user = _require_user(request)
    if user.get("role") not in MANAGER_ROLES:
        raise HTTPException(403, "Только менеджер")
    merged: list[str] = []
    sources: list[str] = []
    if file is not None and getattr(file, "filename", None) and (file.filename or "").strip():
        data = await file.read()
        try:
            emails, src = extract_emails_from_upload(file.filename or "", data)
        except ValueError as e:
            raise HTTPException(400, str(e)) from e
        except ImportError as e:
            raise HTTPException(
                503,
                "На сервере не установлены библиотеки для файлов. В каталоге проекта: "
                "source .venv/bin/activate && pip install -r requirements.txt "
                f"(ошибка: {e})",
            ) from e
        except Exception as e:
            raise HTTPException(
                422,
                f"Не удалось разобрать файл (PDF/Excel). Проверьте формат или размер. Детали: {e}",
            ) from e
        merged.extend(emails)
        sources.append(src)
    url = (sheet_url or "").strip()
    if url:
        try:
            text = fetch_sheet_flat_text(url)
        except ValueError as e:
            raise HTTPException(400, str(e)) from e
        except Exception as e:
            raise HTTPException(502, f"Google Sheets: {e}") from e
        merged.extend(extract_emails_from_text(text))
        sources.append("google_sheet")
    if not sources:
        raise HTTPException(
            400,
            "Прикрепите файл (.pdf, .xlsx, .xls, .csv, .txt) и/или укажите ссылку на Google Таблицу",
        )
    seen: set[str] = set()
    uniq: list[str] = []
    for e in merged:
        el = e.lower().strip()
        if el not in seen:
            seen.add(el)
            uniq.append(el)
    return {"emails": uniq, "count": len(uniq), "sources": sources}


@app.get("/api/mail/status")
async def api_mail_status(request: Request):
    user = _require_user(request)
    if user.get("role") not in MANAGER_ROLES:
        raise HTTPException(403, "Только менеджер")
    s = smtp_settings_from_env()
    return {
        "configured": smtp_configured(),
        "host": s["host"] or None,
        "port": s["port"],
        "use_ssl": s["use_ssl"],
    }


@app.post("/api/mail/send")
async def api_mail_send(
    request: Request,
    emails: str = Form(...),
    subject: str = Form(...),
    body: str = Form(""),
    file: Optional[UploadFile] = File(None),
):
    user = _require_user(request)
    if user.get("role") not in MANAGER_ROLES:
        raise HTTPException(403, "Только менеджер")
    if not smtp_configured():
        raise HTTPException(
            400,
            "SMTP не настроен. В .env укажите SMTP_HOST, SMTP_USER, SMTP_PASSWORD "
            "(опц.: SMTP_FROM, SMTP_FROM_NAME, SMTP_PORT, SMTP_SSL=1 для порта 465).",
        )
    recipients = parse_recipients(emails)
    att_name = None
    att_bytes = None
    att_ct = None
    if file is not None and getattr(file, "filename", None):
        fn = (file.filename or "").strip()
        if fn:
            att_name = fn.split("/")[-1].split("\\")[-1]
            att_bytes = await file.read()
            att_ct = file.content_type
            if not att_bytes:
                att_name = None
                att_bytes = None
                att_ct = None
    result = send_bulk_plain(
        recipients=recipients,
        subject=subject,
        body=body,
        attachment_name=att_name,
        attachment_bytes=att_bytes if att_bytes else None,
        attachment_content_type=att_ct,
    )
    if not result.get("ok") and result.get("sent", 0) == 0:
        err = result.get("error")
        if err:
            raise HTTPException(400, err)
        failed = result.get("failed") or []
        msg = failed[0].get("error", "Ошибка отправки") if failed else "Ошибка отправки"
        raise HTTPException(502, msg)
    return result


STATUS_LABELS = {
    "first_contact": "Установлен первый контакт",
    "negotiation": "Переговоры",
    "waiting": "Ушли в долгое ожидание",
    "proposal_sent": "Выслано КП",
    "closed": "Закрыт",
    "contractors": "Подрядчики",
    "rejected": "Отказ",
    "low_interest": "Не особо интересно",
}


@app.post("/api/local-status")
async def api_set_local_status(data: LocalStatusInput):
    if data.extra_id is not None:
        if not set_local_status_by_id(data.extra_id, data.local_status):
            raise HTTPException(404, "Строка CRM не найдена")
        row = get_row_extra_by_id(data.extra_id)
        if not row:
            raise HTTPException(404, "Строка CRM не найдена")
        phone, sr = row["phone"], row["sheet_row"]
        eid = data.extra_id
    else:
        set_local_status(data.phone, data.sheet_row, data.local_status)
        phone, sr = normalize_crm_phone(data.phone), int(data.sheet_row)
        eid = get_row_extra_id_by_phone_row(phone, sr)
    label = STATUS_LABELS.get(data.local_status, data.local_status)
    add_event(phone, "status_change", f"Статус изменён → {label}", sr)
    return {"ok": True, "extra_id": eid}


@app.post("/api/econom-number")
async def api_set_econom(data: EconomNumberInput):
    if data.extra_id is not None:
        if not set_econom_number_by_id(data.extra_id, data.econom_number or ""):
            raise HTTPException(404, "Строка CRM не найдена")
        row = get_row_extra_by_id(data.extra_id)
        if not row:
            raise HTTPException(404, "Строка CRM не найдена")
        phone, sr = row["phone"], row["sheet_row"]
        eid = data.extra_id
    else:
        set_econom_number(data.phone, data.sheet_row, data.econom_number or "")
        phone, sr = normalize_crm_phone(data.phone), int(data.sheet_row)
        eid = get_row_extra_id_by_phone_row(phone, sr)
    add_event(phone, "name_change", f"Имя изменено → {data.econom_number or '(пусто)'}", sr)
    return {"ok": True, "extra_id": eid}


@app.post("/api/object-info")
async def api_set_object_info(data: ObjectInfoInput):
    if data.extra_id is not None:
        if not set_object_info_by_id(
            data.extra_id, data.address, data.area, data.budget, data.work_type
        ):
            raise HTTPException(404, "Строка CRM не найдена")
        row = get_row_extra_by_id(data.extra_id)
        if not row:
            raise HTTPException(404, "Строка CRM не найдена")
        phone, sr = row["phone"], row["sheet_row"]
        eid = data.extra_id
    else:
        set_object_info(data.phone, data.sheet_row, data.address, data.area, data.budget, data.work_type)
        phone, sr = normalize_crm_phone(data.phone), int(data.sheet_row)
        eid = get_row_extra_id_by_phone_row(phone, sr)
    parts = [p for p in [data.address, data.area, data.budget, data.work_type] if p]
    add_event(phone, "object_update", f"Объект обновлён: {', '.join(parts) or '(пусто)'}", sr)
    return {"ok": True, "extra_id": eid}


@app.post("/api/comment")
async def api_add_comment(data: CommentInput):
    add_comment(data.phone, data.comment, data.sheet_row)
    add_event(data.phone, "comment", data.comment, data.sheet_row)
    return {"ok": True}


@app.get("/api/events/{phone}")
async def api_get_events(phone: str):
    return {"events": get_events(phone)}


@app.delete("/api/events/id/{event_id}")
async def api_delete_event(event_id: int):
    if not delete_event(event_id):
        raise HTTPException(404, "Событие не найдено")
    return {"ok": True}


@app.post("/api/reminder")
async def api_add_reminder(data: ReminderInput):
    add_reminder(data.phone, data.text, data.reminder_at, data.sheet_row, data.recipient_telegram_id)
    add_event(data.phone, "reminder_created", f"Напоминание на {data.reminder_at}: {data.text}", data.sheet_row)
    return {"ok": True}


@app.post("/api/send-now")
async def api_send_now(data: SendNowInput):
    """Отправить сообщение сразу в Telegram."""
    chat_id = data.recipient_telegram_id or None
    extras = get_row_extras()
    key = (data.phone, data.sheet_row)
    extra = extras.get(key, {})
    events = get_events(data.phone, limit=5)
    ok = send_reminder(
        data.phone, data.text, data.sheet_row, chat_id,
        client_name=extra.get("econom_number", ""),
        local_status=extra.get("local_status", ""),
        events=events,
    )
    if ok:
        add_event(data.phone, "message_sent", f"Сообщение отправлено: {data.text}", data.sheet_row)
    return {"ok": ok}


@app.post("/api/contact")
async def api_add_contact(data: ContactInput):
    valid_roles = ("sales_manager", "estimator", "engineer", "sales_head", "test")
    if data.role not in valid_roles:
        raise HTTPException(400, f"Роль: {', '.join(valid_roles)}")
    add_contact(data.name, data.role, data.telegram_id)
    return {"ok": True}


@app.delete("/api/contact/{contact_id}")
async def api_delete_contact(contact_id: int):
    delete_contact(contact_id)
    return {"ok": True}


@app.get("/api/contacts")
async def api_get_contacts():
    return {"contacts": get_all_contacts()}


@app.get("/api/all-tasks")
async def api_all_tasks(request: Request):
    """Все задачи для раздела управления (только менеджер)."""
    user = _require_user(request)
    if user.get("role") not in MANAGER_ROLES:
        raise HTTPException(403, "Доступ только для менеджера")
    tasks = get_all_tasks_for_manager()
    return {"tasks": tasks, "count": len(tasks)}


@app.get("/api/task/{task_id}/forward-context")
async def api_task_forward_context(task_id: int, request: Request):
    """Контекст для «Отправить сметчице»: задача + последний ответ сотрудника (аудит)."""
    user = _require_user(request)
    if user.get("role") not in MANAGER_ROLES:
        raise HTTPException(403, "Доступ только для менеджера")
    task = get_task_by_id(task_id)
    if not task:
        raise HTTPException(404, "Задача не найдена")
    events = get_events(task["phone"], limit=30)
    last_reply = None
    for e in events:
        if e.get("type") == "worker_reply":
            last_reply = e.get("description", "")
            break
    return {
        "task": task,
        "last_worker_reply": last_reply or "",
    }


@app.delete("/api/task/{task_id}")
async def api_delete_task(task_id: int, request: Request):
    """Удалить задачу (только менеджер)."""
    user = _require_user(request)
    if user.get("role") not in MANAGER_ROLES:
        raise HTTPException(403, "Доступ только для менеджера")
    if not delete_task(task_id):
        raise HTTPException(404, "Задача не найдена")
    return {"ok": True}


@app.get("/api/my-tasks")
async def api_my_tasks(request: Request):
    """Задачи текущего сотрудника (по его telegram_id)."""
    user = _require_user(request)
    tg_id = user.get("telegram_id", "")
    if not tg_id:
        return {"tasks": []}
    tasks = get_tasks_for_user(tg_id)
    return {"tasks": tasks}


class TaskStatusInput(BaseModel):
    task_id: int
    status: str


class TaskCommentInput(BaseModel):
    task_id: int
    comment: str


class TaskDelegateInput(BaseModel):
    task_id: int
    recipient_telegram_id: str
    comment: Optional[str] = None


TASK_STATUS_LABELS = {"new": "Новая", "in_progress": "В работе", "done": "Готово"}


@app.post("/api/task-status")
async def api_task_status(data: TaskStatusInput, request: Request):
    """Сотрудник меняет статус задачи."""
    if data.status not in TASK_STATUS_LABELS:
        raise HTTPException(400, "Статус: new, in_progress, done")
    user = _require_user(request)
    task = get_task_by_id(data.task_id)
    if not task:
        raise HTTPException(404, "Задача не найдена")
    update_task_status(data.task_id, data.status)
    label = TASK_STATUS_LABELS[data.status]
    add_event(task["phone"], "task_status", f"{user['name']}: статус задачи → {label}")
    client_line = f"Клиент: {task['phone']}"
    if task.get("client_name"):
        client_line += f" / {task['client_name']}"
    client_line += "\n"
    # Получателю задачи — уведомление с кнопками, чтобы не возвращаться к прошлому сообщению
    recipient_chat = task.get("tg_chat_id")
    if recipient_chat:
        send_task_status_to_recipient(recipient_chat, data.task_id, task, data.status, user["name"])
    # Менеджеру или отправителю подзадачи
    target = task.get("reply_to_chat_id") or TELEGRAM_CHAT_ID
    send_telegram(
        f"📋 Статус задачи обновлён\n\n{client_line}"
        f"Задача: {task['task_text']}\n"
        f"Статус: {label}\n"
        f"От: {user['name']}",
        target,
    )
    return {"ok": True}


WORKER_ROLES = {"estimator", "engineer", "test"}
ROLE_LABELS_DELEGATE = {"estimator": "Сметчица", "engineer": "Инженер", "test": "Тест"}


def _notify_manager_delegate(
    from_name: str,
    from_role: str,
    to_name: str,
    to_role: str,
    phone: str,
    client_name: str = "",
    comment: str = "",
):
    """Оповестить менеджера о запросе на связь между сотрудниками."""
    from_label = ROLE_LABELS_DELEGATE.get(from_role, from_role)
    to_label = ROLE_LABELS_DELEGATE.get(to_role, to_role)
    msg = f"📤 Запрос на связь\n\n"
    msg += f"{from_name} ({from_label}) → {to_name} ({to_label})\n\n"
    msg += f"Клиент: {phone}"
    if client_name:
        msg += f" / {client_name}"
    msg += "\n"
    if comment:
        msg += f"\nСообщение: {comment}"
    send_telegram(msg, TELEGRAM_CHAT_ID)


@app.post("/api/task-delegate")
async def api_task_delegate(data: TaskDelegateInput, request: Request):
    """Сотрудник отправляет запрос на связь другому (сметчица↔инженер). Выбор получателя — на портале."""
    user = _require_user(request)
    if user.get("role") not in WORKER_ROLES:
        raise HTTPException(403, "Доступ только для сметчицы, инженера или теста")
    task = get_task_by_id(data.task_id)
    if not task:
        raise HTTPException(404, "Задача не найдена")
    if task.get("tg_chat_id") != user.get("telegram_id"):
        raise HTTPException(403, "Эта задача не для вас")
    if not data.recipient_telegram_id:
        raise HTTPException(400, "Укажите получателя")
    contacts = get_all_contacts()
    recipient = next((c for c in contacts if c.get("telegram_id") == data.recipient_telegram_id), None)
    if not recipient or not recipient.get("telegram_id"):
        raise HTTPException(400, "Получатель не найден")
    rec_role = recipient.get("role", "")
    if rec_role not in ("estimator", "engineer"):
        raise HTTPException(400, "Можно отправить только сметчице или инженеру")
    if recipient.get("telegram_id") == user.get("telegram_id"):
        raise HTTPException(400, "Нельзя отправить себе")
    comment = (data.comment or "").strip()
    result = send_task_from_worker(
        from_role=user.get("role", "engineer"),
        to_chat_id=data.recipient_telegram_id,
        phone=task["phone"],
        client_name=task.get("client_name", ""),
        comment=comment,
    )
    if not result or (isinstance(result, dict) and result.get("_error")):
        err = result.get("_error", "Ошибка отправки") if isinstance(result, dict) else "Ошибка отправки"
        return {"ok": False, "error": str(err)}
    tg_msg_id = result.get("message_id") if isinstance(result, dict) else None
    task_text = f"Связаться по объекту{f': {comment[:80]}' if comment else ''}"
    if tg_msg_id:
        new_id = save_task_message(
            tg_msg_id, data.recipient_telegram_id, task["phone"], rec_role,
            task_text, parent_task_id=data.task_id,
        )
        if new_id:
            add_task_status_keyboard(data.recipient_telegram_id, tg_msg_id, new_id, rec_role)
    add_event(task["phone"], "task_sent", f"Связь ({user.get('role')}↔{rec_role})", None)
    # Оповещение менеджера
    _notify_manager_delegate(
        from_name=user.get("name", ""),
        from_role=user.get("role", ""),
        to_name=recipient.get("name", ""),
        to_role=rec_role,
        phone=task["phone"],
        client_name=task.get("client_name", ""),
        comment=comment,
    )
    return {"ok": True}


@app.post("/api/task-comment")
async def api_task_comment(data: TaskCommentInput, request: Request):
    """Сотрудник оставляет комментарий к задаче."""
    user = _require_user(request)
    task = get_task_by_id(data.task_id)
    if not task:
        raise HTTPException(404, "Задача не найдена")
    add_event(task["phone"], "worker_comment", f"{user['name']}: {data.comment}")
    client_line = f"Клиент: {task['phone']}"
    if task.get("client_name"):
        client_line += f" / {task['client_name']}"
    client_line += "\n"
    target = task.get("reply_to_chat_id") or TELEGRAM_CHAT_ID
    send_telegram(
        f"💬 Комментарий к задаче\n\n{client_line}"
        f"Задача: {task['task_text'][:80]}\n\n"
        f"{user['name']}:\n{data.comment}",
        target,
    )
    return {"ok": True}


@app.post("/api/weekly-report")
async def api_send_weekly_report(request: Request):
    """Отправить еженедельный отчёт себе на проверку (только менеджер). Перешлите РОП после проверки."""
    user = _require_user(request)
    if user.get("role") not in MANAGER_ROLES:
        raise HTTPException(403, "Доступ только для менеджера")
    report = _build_weekly_report()
    if not report:
        raise HTTPException(400, "Не удалось сформировать отчёт (проверьте GOOGLE_SHEET_URL)")
    tg_id = user.get("telegram_id")
    if not tg_id:
        raise HTTPException(400, "У вас не указан telegram_id. Добавьте себя в команду и напишите боту /start.")
    if not send_weekly_report(tg_id, report):
        raise HTTPException(400, "Не удалось отправить. Напишите боту /start.")
    return {"ok": True}


@app.post("/api/task")
async def api_send_task(request: Request):
    """Отправка задачи. Форма вручную — Form+File вместе ломали парсинг."""
    try:
        form = await request.form()
        phone = form.get("phone") or ""
        object_info = form.get("object_info") or ""
        task = form.get("task") or ""
        role = form.get("role") or "sales_manager"
        recipient_telegram_id = form.get("recipient_telegram_id") or None
        if not phone or not task:
            raise HTTPException(400, "Укажите телефон и задачу")
        user = _require_user(request)
        if user.get("role") not in MANAGER_ROLES:
            raise HTTPException(403, "Доступ только для менеджера")
        chat_id = recipient_telegram_id
        if not chat_id:
            contacts = get_contacts_by_role(role)
            if not contacts:
                raise HTTPException(400, f"Нет контактов с ролью {role}")
            chat_id = contacts[0].get("telegram_id")
        if not chat_id:
            raise HTTPException(400, "Укажите telegram_id получателя или добавьте контакт")
        events = get_events(phone, limit=5)
        result = send_task_to_role(role, object_info, task, chat_id, events=events)
        ok = not (isinstance(result, dict) and "_error" in result) and bool(result)
        err_msg = result.get("_error") if isinstance(result, dict) and "_error" in result else None
        if ok and isinstance(result, dict) and "_error" not in result:
            tg_msg_id = result.get("message_id")
            if tg_msg_id:
                task_id = save_task_message(tg_msg_id, chat_id, phone, role, task)
                if task_id:
                    add_task_status_keyboard(chat_id, tg_msg_id, task_id, role)
        files_to_send = []
        for key in ("file", "files"):
            for v in form.getlist(key):
                if v and hasattr(v, "filename") and hasattr(v, "read"):
                    files_to_send.append(v)
        if ok and files_to_send:
            for f in files_to_send:
                content = await f.read()
                if content:
                    if len(content) > 10 * 1024 * 1024:
                        raise HTTPException(400, f"Файл не более 10 МБ")
                    fn = getattr(f, "filename", None) or "file"
                    is_image = fn.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".webp"))
                    if is_image:
                        send_photo(chat_id, content, "📎 Файл к задаче")
                    else:
                        send_document(chat_id, content, fn, "📎 Файл к задаче")
        if ok:
            add_event(phone, "task_sent", f"Задача ({role}): {task}", None)
        if err_msg and "can't initiate conversation" in (err_msg or "").lower():
            err_msg = "Получатель не писал боту /start. Попросите инженера открыть бота и отправить /start."
        return {"ok": ok, "error": err_msg}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
