"""Telegram-бот на aiogram — хендлеры команд и ответов."""
import html
import logging
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, ErrorEvent, LinkPreviewOptions

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, APP_BASE_URL, LOGIN_LINK_URL
from database import (
    get_all_contacts,
    get_contacts_by_role,
    find_task_by_message,
    find_last_task_for_chat,
    add_event,
    update_task_status,
    get_task_by_id,
    save_task_message,
)
from auth_tokens import create as create_login_token
from telegram_bot import send_telegram, forward_reply_to_manager, send_task_from_worker, add_task_status_keyboard, send_task_status_to_recipient


def _get_login_link(token: str) -> str:
    """Ссылка для входа. LOGIN_LINK_URL — для кликабельной кнопки (ngrok/домен)."""
    base = (LOGIN_LINK_URL or APP_BASE_URL).rstrip("/")
    # localhost не кликабелен в Telegram — заменяем на 127.0.0.1 (работает на том же ПК)
    if "localhost" in base.lower():
        base = base.replace("localhost", "127.0.0.1").replace("LOCALHOST", "127.0.0.1")
    return f"{base}/auth/{token}"


def _build_login_keyboard(link: str) -> InlineKeyboardMarkup:
    """Кнопка входа — всегда, Telegram принимает http/https."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Ссылка для входа в Integra Sales", url=link)]
    ])


async def cmd_start_login(message: Message):
    """Обработка /start и /login."""
    chat_id = str(message.chat.id)
    contacts = get_all_contacts()
    contact = next((c for c in contacts if c["telegram_id"] == chat_id), None)
    if not contact:
        await message.answer(
            "❌ Ваш Telegram ID не найден в системе.\nПопросите менеджера добавить вас в команду."
        )
        return
    token = create_login_token(contact, chat_id)
    link = _get_login_link(token)
    keyboard = _build_login_keyboard(link)
    base = LOGIN_LINK_URL or APP_BASE_URL
    if base.startswith("https://"):
        safe_url = link.replace("&", "&amp;")
        safe_name = html.escape(contact["name"])
        text = (
            f"👋 Привет, {safe_name}!\n\n"
            f'<a href="{safe_url}">Ссылка для входа в Integra Sales</a>\n\n'
            f"Действительна 30 минут."
        )
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard, link_preview_options=LinkPreviewOptions(is_disabled=True))
    else:
        text = (
            f"👋 Привет, {contact['name']}!\n\n"
            f"Нажмите кнопку ниже для входа.\n\n"
            f"Действительна 30 минут."
        )
        await message.answer(text, reply_markup=keyboard, link_preview_options=LinkPreviewOptions(is_disabled=True))


TASK_STATUS_LABELS = {"new": "Новая", "in_progress": "В работе", "done": "Готово"}
STATUS_TEXT_MAP = {"в работе": "in_progress", "готово": "done", "в работе!": "in_progress", "готово!": "done"}


def _format_client_line(phone: str, client_name: str = "") -> str:
    """Форматировать строку клиента: телефон и имя при наличии."""
    line = f"Клиент: {phone}"
    if client_name:
        line += f" / {client_name}"
    return line + "\n"


def _do_task_status_update(task_info: dict, status: str, worker_name: str) -> bool:
    """Обновить статус задачи и уведомить получателя (с кнопками) + менеджера."""
    task_id = task_info.get("id")
    if not task_id:
        return False
    update_task_status(task_id, status)
    label = TASK_STATUS_LABELS.get(status, status)
    add_event(task_info["phone"], "task_status", f"{worker_name}: статус задачи → {label}")
    # Получателю задачи — новое сообщение с кнопками
    recipient_chat = task_info.get("tg_chat_id")
    if recipient_chat:
        send_task_status_to_recipient(recipient_chat, task_id, task_info, status, worker_name)
    # Менеджеру или отправителю подзадачи
    client_line = _format_client_line(task_info.get("phone", ""), task_info.get("client_name", ""))
    msg = (
        f"📋 Статус задачи обновлён\n\n"
        f"{client_line}"
        f"Задача: {task_info['task_text'][:80]}\n"
        f"Статус: {label}\n"
        f"От: {worker_name}"
    )
    target = task_info.get("reply_to_chat_id") or TELEGRAM_CHAT_ID
    send_telegram(msg, target)
    return True


async def handle_worker_reply(message: Message):
    """Ответ работника на задачу — смена статуса или комментарий менеджеру."""
    chat_id = str(message.chat.id)
    if chat_id == TELEGRAM_CHAT_ID:
        return
    sender = message.from_user
    worker_name = (sender.first_name or "") + (f" {sender.last_name}" if sender.last_name else "")
    worker_name = worker_name.strip() or "Неизвестный"
    text = (message.text or "").strip()
    if not text:
        return
    task_info = None
    if message.reply_to_message:
        task_info = find_task_by_message(message.reply_to_message.message_id, chat_id)
    if not task_info:
        task_info = find_last_task_for_chat(chat_id)
    if not task_info:
        return
    text_lower = text.lower()
    if text_lower in STATUS_TEXT_MAP:
        status = STATUS_TEXT_MAP[text_lower]
        if _do_task_status_update(task_info, status, worker_name):
            await message.reply(f"✅ Статус: {TASK_STATUS_LABELS[status]}")
        return
    result = forward_reply_to_manager(
        worker_name=worker_name,
        reply_text=text,
        phone=task_info["phone"],
        original_task=task_info["task_text"],
        client_name=task_info.get("client_name", ""),
        reply_to_chat_id=task_info.get("reply_to_chat_id"),
    )
    # Для обмена сметчица↔инженер↔инженер: сохраняем сообщение как задачу, чтобы получатель мог ответить
    reply_to = task_info.get("reply_to_chat_id")
    if reply_to and reply_to != TELEGRAM_CHAT_ID and isinstance(result, dict) and result.get("message_id"):
        parent = get_task_by_id(task_info["parent_task_id"]) if task_info.get("parent_task_id") else None
        recipient_role = parent["role"] if parent else ("estimator" if task_info.get("role") == "engineer" else "engineer")
        save_task_message(
            result["message_id"], reply_to, task_info["phone"], recipient_role,
            f"Ответ от {worker_name}: {text[:100]}",
            parent_task_id=task_info.get("id"),
        )
    add_event(
        task_info["phone"],
        "worker_reply",
        f"Ответ от {worker_name}: {text}",
    )


async def handle_worker_file(message: Message):
    """Файл/фото/видео от работника — переслать менеджеру."""
    chat_id = str(message.chat.id)
    if chat_id == TELEGRAM_CHAT_ID:
        return
    sender = message.from_user
    worker_name = (sender.first_name or "") + (f" {sender.last_name}" if sender.last_name else "")
    worker_name = worker_name.strip() or "Неизвестный"
    task_info = None
    if message.reply_to_message:
        task_info = find_task_by_message(message.reply_to_message.message_id, chat_id)
    if not task_info:
        task_info = find_last_task_for_chat(chat_id)
    if not task_info:
        return
    caption = message.caption or ""
    target_chat = task_info.get("reply_to_chat_id") or TELEGRAM_CHAT_ID
    file_reply_text = f"📎 Файл" + (f": {caption}" if caption else "")
    result = forward_reply_to_manager(
        worker_name=worker_name,
        reply_text=file_reply_text,
        phone=task_info["phone"],
        original_task=task_info["task_text"],
        reply_to_chat_id=task_info.get("reply_to_chat_id"),
    )
    reply_to = task_info.get("reply_to_chat_id")
    if reply_to and reply_to != TELEGRAM_CHAT_ID and isinstance(result, dict) and result.get("message_id"):
        parent = get_task_by_id(task_info["parent_task_id"]) if task_info.get("parent_task_id") else None
        recipient_role = parent["role"] if parent else ("estimator" if task_info.get("role") == "engineer" else "engineer")
        save_task_message(
            result["message_id"], reply_to, task_info["phone"], recipient_role,
            f"Файл от {worker_name}",
            parent_task_id=task_info.get("id"),
        )
    try:
        await message.forward(chat_id=target_chat)
    except Exception:
        pass
    add_event(
        task_info["phone"],
        "worker_reply",
        f"Файл от {worker_name}" + (f": {caption[:50]}…" if len(caption) > 50 else (f": {caption}" if caption else "")),
    )


async def handle_task_status_callback(callback: CallbackQuery):
    """Обработка нажатия кнопки смены статуса задачи."""
    data = callback.data or ""
    if not data.startswith("task_status:"):
        return
    parts = data.split(":")
    if len(parts) != 3:
        await callback.answer("Ошибка")
        return
    _, task_id_str, status = parts
    if status not in TASK_STATUS_LABELS:
        await callback.answer("Неизвестный статус")
        return
    try:
        task_id = int(task_id_str)
    except ValueError:
        await callback.answer("Ошибка")
        return
    task = get_task_by_id(task_id)
    if not task:
        await callback.answer("Задача не найдена")
        return
    chat_id = str(callback.message.chat.id) if callback.message else ""
    if task.get("tg_chat_id") != chat_id:
        await callback.answer("Эта задача не для вас")
        return
    update_task_status(task_id, status)
    label = TASK_STATUS_LABELS[status]
    sender = callback.from_user
    worker_name = (sender.first_name or "") + (f" {sender.last_name}" if sender.last_name else "")
    worker_name = worker_name.strip() or "Сотрудник"
    add_event(task["phone"], "task_status", f"{worker_name}: статус задачи → {label}")
    # Получателю задачи — новое сообщение с кнопками, чтобы не возвращаться к прошлому
    send_task_status_to_recipient(chat_id, task_id, task, status, worker_name)
    # Менеджеру или отправителю подзадачи
    client_line = _format_client_line(task.get("phone", ""), task.get("client_name", ""))
    msg = (
        f"📋 Статус задачи обновлён\n\n"
        f"{client_line}"
        f"Задача: {task['task_text'][:80]}\n"
        f"Статус: {label}\n"
        f"От: {worker_name}"
    )
    target = task.get("reply_to_chat_id") or TELEGRAM_CHAT_ID
    send_telegram(msg, target)
    await callback.answer(f"Статус: {label}")


# target_role -> from_role (кто нажал кнопку), если не задано в task.role
DELEGATE_FROM = {"engineer": "estimator", "estimator": "engineer", "engineer_other": "engineer"}


async def handle_task_delegate_callback(callback: CallbackQuery):
    """Сметчица↔инженер или инженер↔инженер."""
    data = callback.data or ""
    if not data.startswith("task_delegate:"):
        return
    parts = data.split(":")
    if len(parts) != 3:
        await callback.answer("Ошибка")
        return
    _, task_id_str, target_role = parts
    if target_role not in ("engineer", "estimator", "engineer_other"):
        await callback.answer("Неизвестный получатель")
        return
    try:
        task_id = int(task_id_str)
    except ValueError:
        await callback.answer("Ошибка")
        return
    task = get_task_by_id(task_id)
    if not task:
        await callback.answer("Задача не найдена")
        return
    chat_id = str(callback.message.chat.id) if callback.message else ""
    if task.get("tg_chat_id") != chat_id:
        await callback.answer("Эта задача не для вас")
        return
    from_role = task.get("role") or DELEGATE_FROM.get(target_role, "engineer")
    lookup_role = "engineer" if target_role == "engineer_other" else target_role
    contacts = get_contacts_by_role(lookup_role)
    # Для engineer_other исключаем текущего инженера
    if target_role == "engineer_other":
        contacts = [c for c in contacts if c.get("telegram_id") != chat_id]
    if not contacts or not contacts[0].get("telegram_id"):
        await callback.answer("Нет другого инженера" if target_role == "engineer_other" else f"Нет контактов с ролью {lookup_role}")
        return
    to_chat_id = contacts[0]["telegram_id"]
    if to_chat_id == chat_id:
        await callback.answer("Нельзя отправить задачу себе")
        return
    result = send_task_from_worker(
        from_role=from_role,
        to_chat_id=to_chat_id,
        phone=task["phone"],
        client_name=task.get("client_name", ""),
    )
    if not result or (isinstance(result, dict) and result.get("_error")):
        await callback.answer("Не удалось отправить")
        return
    tg_msg_id = result.get("message_id") if isinstance(result, dict) else None
    if tg_msg_id:
        save_role = "engineer" if target_role == "engineer_other" else target_role
        new_id = save_task_message(
            tg_msg_id, to_chat_id, task["phone"], save_role, "Связаться по объекту", parent_task_id=task_id
        )
        if new_id:
            add_task_status_keyboard(to_chat_id, tg_msg_id, new_id, save_role)
    add_event(task["phone"], "task_sent", f"Связь ({from_role}↔{lookup_role})", None)
    role_names = {"engineer": "инженеру", "engineer_other": "инженеру", "estimator": "сметчице"}
    await callback.answer(f"Отправлено {role_names.get(target_role, target_role)}")


async def errors_handler(event: ErrorEvent):
    """Логировать ошибки бота."""
    logging.exception("Bot error: %s", event.exception)


def setup_handlers(dp: Dispatcher):
    """Регистрация хендлеров."""
    dp.error.register(errors_handler)
    dp.message.register(cmd_start_login, Command("start"))
    dp.callback_query.register(handle_task_status_callback, F.data.startswith("task_status:"))
    dp.callback_query.register(handle_task_delegate_callback, F.data.startswith("task_delegate:"))
    manager_id = int(TELEGRAM_CHAT_ID) if TELEGRAM_CHAT_ID else 0
    dp.message.register(handle_worker_reply, F.text, F.chat.id != manager_id)
    dp.message.register(
        handle_worker_file,
        (F.document | F.photo | F.video | F.audio | F.voice | F.video_note),
        F.chat.id != manager_id,
    )


async def run_polling():
    """Запуск long-polling бота."""
    if not TELEGRAM_BOT_TOKEN:
        logging.warning("TELEGRAM_BOT_TOKEN не задан — бот не запущен")
        return
    logging.basicConfig(level=logging.INFO)
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    dp = Dispatcher()
    setup_handlers(dp)
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
