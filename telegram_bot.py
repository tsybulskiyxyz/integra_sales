"""Отправка уведомлений в Telegram."""
import html
import httpx
from typing import Optional

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID


STATUS_LABELS = {
    "first_contact": "Установлен первый контакт",
    "negotiation": "Переговоры",
    "waiting": "Ушли в долгое ожидание",
    "proposal_sent": "Выслано КП",
    "closed": "Закрыт",
    "rejected": "Отказ",
}

EVENT_LABELS = {
    "comment": "Комментарий",
    "status_change": "Статус",
    "name_change": "Имя",
    "reminder_created": "Напоминание",
    "reminder_sent": "Отправлено",
    "message_sent": "Сообщение",
    "task_sent": "Задача",
    "worker_reply": "Ответ работника",
}


def send_telegram(message: str, chat_id: Optional[str] = None, reply_markup: Optional[dict] = None, parse_mode: Optional[str] = None) -> dict | bool:
    """Отправить сообщение в Telegram. Возвращает dict с result (включая message_id) или False."""
    token = TELEGRAM_BOT_TOKEN
    target = chat_id or TELEGRAM_CHAT_ID
    if not token or not target:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": target, "text": message}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        with httpx.Client() as client:
            r = client.post(url, json=payload)
            if r.status_code == 200:
                data = r.json()
                return data.get("result", True)
            err = r.json() if r.content else {}
            desc = err.get("description", str(r.status_code))
            return {"_error": desc}
    except Exception as e:
        return {"_error": str(e)}




def set_bot_commands():
    """Зарегистрировать команды бота в меню Telegram."""
    token = TELEGRAM_BOT_TOKEN
    if not token:
        return
    url = f"https://api.telegram.org/bot{token}/setMyCommands"
    commands = [
        {"command": "start", "description": "Ссылка для входа в Integra Sales"},
    ]
    try:
        with httpx.Client() as client:
            client.post(url, json={"commands": commands})
    except Exception:
        pass


def get_updates(offset: int = 0, timeout: int = 1) -> tuple[list[dict], int]:
    """Получить новые сообщения от бота (long-polling). Возвращает (updates, new_offset)."""
    token = TELEGRAM_BOT_TOKEN
    if not token:
        return [], offset
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    try:
        with httpx.Client(timeout=timeout + 5) as client:
            r = client.post(url, json={"offset": offset, "timeout": timeout})
            if r.status_code != 200:
                return [], offset
            data = r.json()
            updates = data.get("result", [])
            new_offset = offset
            if updates:
                new_offset = updates[-1]["update_id"] + 1
            return updates, new_offset
    except Exception:
        return [], offset


def _format_history(events: list[dict], max_items: int = 5) -> str:
    """Форматирует последние события в читаемый текст."""
    if not events:
        return "  Нет записей"
    lines = []
    for e in events[:max_items]:
        label = EVENT_LABELS.get(e["type"], e["type"])
        ts = e.get("created_at", "")
        if ts:
            ts = ts[:16].replace("T", " ")
        lines.append(f"  [{ts}] {label}: {e['description']}")
    return "\n".join(lines)


def send_reminder(
    phone: str,
    text: str,
    sheet_row: Optional[int] = None,
    recipient_chat_id: Optional[str] = None,
    client_name: str = "",
    local_status: str = "",
    events: Optional[list[dict]] = None,
):
    """Отправить напоминание с полной карточкой клиента."""
    status_label = STATUS_LABELS.get(local_status, local_status) if local_status else ""

    msg = f"📞 Напоминание\n\n"
    msg += f"Контакт: {phone}\n"
    if client_name:
        msg += f"Имя: {client_name}\n"
    if status_label:
        msg += f"Статус: {status_label}\n"
    msg += f"\nЗадача:\n{text}\n"

    if events:
        msg += f"\nИстория:\n{_format_history(events)}"

    result = send_telegram(msg, recipient_chat_id)
    return bool(result)


def send_task_to_role(
    role: str,
    object_info: str,
    task: str,
    recipient_telegram_id: Optional[str] = None,
    events: Optional[list[dict]] = None,
) -> dict | bool:
    """Отправить задачу исполнителю. Возвращает dict с message_id при успехе, False при ошибке."""
    role_names = {
        "sales_manager": "Менеджер по продажам",
        "estimator": "Сметчица",
        "engineer": "Инженер",
        "sales_head": "Руководитель отдела продаж",
    }
    msg = f"📋 Задача для {role_names.get(role, role)}\n\n"
    msg += f"Объект:\n{object_info}\n\n"
    msg += f"Задача:\n{task}"
    msg += "\n\n💬 Ответьте текстом — комментарий менеджеру. Напишите «В работе» или «Готово» — смена статуса."

    if events:
        msg += f"\n\nИстория:\n{_format_history(events)}"

    return send_telegram(msg, recipient_telegram_id)


def send_task_reminder(chat_id: str, task_text: str, phone: str, client_name: str = "") -> bool:
    """Напоминание сотруднику о незавершённой задаче."""
    msg = "⏰ Напоминание о задаче\n\n"
    msg += f"Клиент: {phone}"
    if client_name:
        msg += f" / {client_name}"
    msg += f"\n\nЗадача:\n{task_text}\n\n"
    msg += "Отметьте задачу «Готово», когда выполните."
    return bool(send_telegram(msg, chat_id))


def send_document(chat_id: str, file_content: bytes, filename: str, caption: str = "") -> dict | bool:
    """Отправить документ в Telegram. Возвращает dict с message_id при успехе."""
    token = TELEGRAM_BOT_TOKEN
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    try:
        with httpx.Client() as client:
            files = {"document": (filename, file_content)}
            data = {"chat_id": chat_id}
            if caption:
                data["caption"] = caption[:1024]
            r = client.post(url, data=data, files=files)
            if r.status_code == 200:
                return r.json().get("result", True)
            return False
    except Exception:
        return False


def _build_task_keyboard(task_id: int, role: str = "") -> dict:
    """Собрать inline-кнопки только смены статуса. Делегирование — на портале CRM."""
    return {
        "inline_keyboard": [[
            {"text": "🔹 В работе", "callback_data": f"task_status:{task_id}:in_progress"},
            {"text": "✅ Готово", "callback_data": f"task_status:{task_id}:done"},
        ]]
    }


def send_task_status_to_recipient(chat_id: str, task_id: int, task: dict, status: str, worker_name: str) -> bool:
    """Отправить получателю задачи уведомление об изменении статуса с кнопками — чтобы не возвращаться к прошлому сообщению."""
    if not chat_id:
        return False
    client_line = f"Клиент: {task.get('phone', '')}"
    if task.get("client_name"):
        client_line += f" / {task['client_name']}"
    client_line += "\n"
    label = {"new": "Новая", "in_progress": "В работе", "done": "Готово"}.get(status, status)
    msg = (
        f"📋 Статус задачи: {label}\n\n"
        f"{client_line}"
        f"Задача: {task.get('task_text', '')[:80]}\n\n"
        f"Статус изменён: {worker_name}"
    )
    role = task.get("role", "")
    result = send_telegram(msg, chat_id, reply_markup=_build_task_keyboard(task_id, role))
    return bool(result) and not (isinstance(result, dict) and "_error" in result)


def add_task_status_keyboard(chat_id: str, message_id: int, task_id: int, role: str = "") -> bool:
    """Добавить inline-кнопки только смены статуса (В работе / Готово). Делегирование — на портале."""
    token = TELEGRAM_BOT_TOKEN
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/editMessageReplyMarkup"
    reply_markup = _build_task_keyboard(task_id, role)
    try:
        with httpx.Client() as client:
            r = client.post(url, json={
                "chat_id": chat_id,
                "message_id": message_id,
                "reply_markup": reply_markup,
            })
            return r.status_code == 200
    except Exception:
        return False


def send_photo(chat_id: str, file_content: bytes, caption: str = "") -> dict | bool:
    """Отправить фото в Telegram."""
    token = TELEGRAM_BOT_TOKEN
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    try:
        with httpx.Client() as client:
            files = {"photo": ("image.jpg", file_content)}
            data = {"chat_id": chat_id}
            if caption:
                data["caption"] = caption[:1024]
            r = client.post(url, data=data, files=files)
            if r.status_code == 200:
                return r.json().get("result", True)
            return False
    except Exception:
        return False


def send_weekly_report(chat_id: str, report_text: str) -> bool:
    """Отправить еженедельный отчёт (на проверку — перешлите РОП)."""
    msg = f"📊 Отчёт по звонкам за неделю\n\n{report_text}\n\nПерешлите РОП после проверки."
    result = send_telegram(msg, chat_id)
    return bool(result) and not (isinstance(result, dict) and "_error" in result)


def forward_reply_to_manager(
    worker_name: str,
    reply_text: str,
    phone: str,
    original_task: str,
    client_name: str = "",
    reply_to_chat_id: Optional[str] = None,
) -> dict | bool:
    """Переслать ответ работника менеджеру или отправителю подзадачи. Возвращает результат send_telegram."""
    msg = f"💬 Ответ от {worker_name}\n\n"
    msg += f"Клиент: {phone}"
    if client_name:
        msg += f" / {client_name}"
    msg += f"\nЗадача: {original_task}\n\n"
    msg += f"Комментарий:\n{reply_text}"
    target = reply_to_chat_id or TELEGRAM_CHAT_ID
    return send_telegram(msg, target)


def send_task_from_worker(
    from_role: str,
    to_chat_id: str,
    phone: str,
    client_name: str = "",
) -> dict | bool:
    """Отправить запрос на связь от сметчицы/инженера другому. Возвращает dict с message_id."""
    role_names = {"estimator": "Сметчица", "engineer": "Инженер", "test": "Тест"}
    from_name = role_names.get(from_role, from_role)
    object_info = f"Контакт: {phone}"
    if client_name:
        object_info += f"\nИмя: {client_name}"
    msg = f"📋 {from_name} хочет связаться по объекту\n\n"
    msg += f"{object_info}\n\n"
    msg += "💬 Ответьте в чате — ваше сообщение придёт отправителю."
    return send_telegram(msg, to_chat_id)


