"""Локальная БД SQLite."""
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

from config import DATABASE_PATH


def get_connection():
    """Создаёт соединение с БД."""
    Path(DATABASE_PATH).parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DATABASE_PATH)


def init_db():
    """Инициализация таблиц."""
    conn = get_connection()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT NOT NULL,
                sheet_row INTEGER,
                comment TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT NOT NULL,
                sheet_row INTEGER,
                reminder_text TEXT NOT NULL,
                reminder_at TEXT NOT NULL,
                sent INTEGER DEFAULT 0,
                recipient_telegram_id TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                telegram_id TEXT,
                role TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS status_overrides (
                phone TEXT NOT NULL,
                sheet_row INTEGER NOT NULL,
                status TEXT NOT NULL,
                PRIMARY KEY (phone, sheet_row)
            );
            CREATE TABLE IF NOT EXISTS row_extras (
                phone TEXT NOT NULL,
                sheet_row INTEGER NOT NULL,
                econom_number TEXT,
                local_status TEXT DEFAULT 'first_contact',
                object_address TEXT DEFAULT '',
                object_area TEXT DEFAULT '',
                object_budget TEXT DEFAULT '',
                object_work_type TEXT DEFAULT '',
                PRIMARY KEY (phone, sheet_row)
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT NOT NULL,
                sheet_row INTEGER,
                event_type TEXT NOT NULL,
                description TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_comments_phone ON comments(phone);
            CREATE INDEX IF NOT EXISTS idx_reminders_phone ON reminders(phone);
            CREATE INDEX IF NOT EXISTS idx_reminders_at ON reminders(reminder_at);
            CREATE INDEX IF NOT EXISTS idx_events_phone ON events(phone);
            CREATE TABLE IF NOT EXISTS task_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_message_id INTEGER NOT NULL,
                tg_chat_id TEXT NOT NULL,
                phone TEXT NOT NULL,
                role TEXT,
                task_text TEXT,
                status TEXT DEFAULT 'new',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_task_msg ON task_messages(tg_message_id, tg_chat_id);
        """)
        conn.commit()
        try:
            conn.execute("ALTER TABLE task_messages ADD COLUMN status TEXT DEFAULT 'new'")
            conn.commit()
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE task_messages ADD COLUMN last_reminder_at TEXT")
            conn.commit()
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE task_messages ADD COLUMN parent_task_id INTEGER REFERENCES task_messages(id)")
            conn.commit()
        except Exception:
            pass
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS auth_tokens (
                token TEXT PRIMARY KEY,
                contact_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                role TEXT NOT NULL,
                telegram_id TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_auth_tokens_expires ON auth_tokens(expires_at);
            CREATE TABLE IF NOT EXISTS legal_leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_name TEXT NOT NULL DEFAULT '',
                inn TEXT DEFAULT '',
                phone TEXT DEFAULT '',
                email TEXT DEFAULT '',
                okved TEXT DEFAULT '',
                region TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pool',
                notes TEXT DEFAULT '',
                source TEXT DEFAULT 'manual',
                next_contact_at TEXT DEFAULT '',
                priority INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_legal_leads_status ON legal_leads(status);
            CREATE TABLE IF NOT EXISTS legal_lead_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL,
                event_type TEXT NOT NULL DEFAULT 'note',
                description TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_legal_lead_events_lead ON legal_lead_events(lead_id);
        """)
        conn.commit()
        try:
            conn.execute("ALTER TABLE legal_leads ADD COLUMN next_contact_at TEXT DEFAULT ''")
            conn.commit()
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE legal_leads ADD COLUMN priority INTEGER DEFAULT 0")
            conn.commit()
        except Exception:
            pass
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS crm_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
        """)
        conn.commit()
        _migrate_legal_lead_statuses(conn)
        conn.commit()
    finally:
        conn.close()


def _migrate_legal_lead_statuses(conn):
    """Старые статусы воронки → база / обход / квалиф. / стоп."""
    mapping = (
        ("new", "pool"),
        ("contacted", "pool"),
        ("engaged", "outreach"),
        ("qualified", "qualified"),
        ("lost", "stop"),
    )
    for old, new in mapping:
        conn.execute("UPDATE legal_leads SET status = ? WHERE status = ?", (new, old))


CRM_SETTING_LEGAL_SHEET_URL = "legal_sheet_url"


def get_crm_setting(key: str) -> str:
    conn = get_connection()
    try:
        row = conn.execute("SELECT value FROM crm_settings WHERE key = ?", (key,)).fetchone()
        return (row[0] if row else "") or ""
    finally:
        conn.close()


def set_crm_setting(key: str, value: str):
    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO crm_settings (key, value) VALUES (?, ?)
               ON CONFLICT(key) DO UPDATE SET value = excluded.value""",
            (key, value or ""),
        )
        conn.commit()
    finally:
        conn.close()


def save_auth_token(token: str, contact_id: int, name: str, role: str, telegram_id: str, expires_at: str):
    """Сохранить токен входа в БД."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO auth_tokens (token, contact_id, name, role, telegram_id, expires_at) VALUES (?, ?, ?, ?, ?, ?)",
            (token, contact_id, name, role, telegram_id, expires_at),
        )
        conn.commit()
    finally:
        conn.close()


def consume_auth_token(token: str) -> Optional[dict]:
    """Проверить токен. Возвращает {contact, telegram_id} или None.
    Токен НЕ удаляется — иначе Telegram prefetch (при показе превью ссылки) съедает его до клика пользователя."""
    conn = get_connection()
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row = conn.execute(
            "SELECT contact_id, name, role, telegram_id FROM auth_tokens WHERE token = ? AND expires_at > ?",
            (token, now),
        ).fetchone()
        if not row:
            return None
        return {
            "contact": {"id": row[0], "name": row[1], "role": row[2], "telegram_id": row[3]},
            "telegram_id": row[3],
        }
    finally:
        conn.close()


def add_comment(phone: str, comment: str, sheet_row: Optional[int] = None):
    """Добавить комментарий к контакту."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO comments (phone, sheet_row, comment) VALUES (?, ?, ?)",
            (phone, sheet_row, comment)
        )
        conn.commit()
    finally:
        conn.close()


def get_comments(phone: str) -> list[dict]:
    """Получить комментарии по телефону."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT id, comment, sheet_row, created_at FROM comments WHERE phone = ? ORDER BY created_at DESC",
            (phone,)
        ).fetchall()
        return [{"id": r[0], "comment": r[1], "sheet_row": r[2], "created_at": r[3]} for r in rows]
    finally:
        conn.close()


def add_reminder(phone: str, reminder_text: str, reminder_at: str, sheet_row: Optional[int] = None, recipient_telegram_id: Optional[str] = None):
    """Добавить напоминание."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO reminders (phone, sheet_row, reminder_text, reminder_at, recipient_telegram_id) VALUES (?, ?, ?, ?, ?)",
            (phone, sheet_row, reminder_text, reminder_at, recipient_telegram_id)
        )
        conn.commit()
    finally:
        conn.close()


def get_pending_reminders() -> list[dict]:
    """Получить напоминания, которые ещё не отправлены и время наступило."""
    conn = get_connection()
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = conn.execute(
            "SELECT id, phone, sheet_row, reminder_text, reminder_at, recipient_telegram_id FROM reminders WHERE sent = 0 AND reminder_at <= ? ORDER BY reminder_at",
            (now,)
        ).fetchall()
        return [{"id": r[0], "phone": r[1], "sheet_row": r[2], "text": r[3], "at": r[4], "recipient": r[5]} for r in rows]
    finally:
        conn.close()


def get_todays_reminders() -> list[dict]:
    """Напоминания на сегодня (неотправленные, reminder_at до конца дня)."""
    conn = get_connection()
    try:
        now = datetime.now()
        today_start = now.strftime("%Y-%m-%d 00:00:00")
        today_end = now.strftime("%Y-%m-%d 23:59:59")
        rows = conn.execute(
            """SELECT id, phone, sheet_row, reminder_text, reminder_at, recipient_telegram_id
               FROM reminders WHERE sent = 0 AND reminder_at >= ? AND reminder_at <= ?
               ORDER BY reminder_at""",
            (today_start, today_end)
        ).fetchall()
        return [{"id": r[0], "phone": r[1], "sheet_row": r[2], "text": r[3], "at": r[4], "recipient": r[5]} for r in rows]
    finally:
        conn.close()


def get_overdue_reminders() -> list[dict]:
    """Просроченные напоминания (неотправленные, reminder_at < сейчас)."""
    conn = get_connection()
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = conn.execute(
            """SELECT id, phone, sheet_row, reminder_text, reminder_at, recipient_telegram_id
               FROM reminders WHERE sent = 0 AND reminder_at < ?
               ORDER BY reminder_at""",
            (now,)
        ).fetchall()
        return [{"id": r[0], "phone": r[1], "sheet_row": r[2], "text": r[3], "at": r[4], "recipient": r[5]} for r in rows]
    finally:
        conn.close()


def get_status_summary() -> dict:
    """Сводка по статусам клиентов."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT local_status, COUNT(*) FROM row_extras GROUP BY local_status"
        ).fetchall()
        result = {}
        for status, cnt in rows:
            result[status or "first_contact"] = cnt
        return result
    finally:
        conn.close()


def mark_reminder_sent(reminder_id: int):
    """Отметить напоминание как отправленное."""
    conn = get_connection()
    try:
        conn.execute("UPDATE reminders SET sent = 1 WHERE id = ?", (reminder_id,))
        conn.commit()
    finally:
        conn.close()


def add_contact(name: str, role: str, telegram_id: Optional[str] = None):
    """Добавить контакт (исполнитель, сметчица, инженер)."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO contacts (name, telegram_id, role) VALUES (?, ?, ?)",
            (name, telegram_id, role)
        )
        conn.commit()
    finally:
        conn.close()


def get_contacts_by_role(role: str) -> list[dict]:
    """Получить контакты по роли: executor, estimator, engineer."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT id, name, telegram_id FROM contacts WHERE role = ?",
            (role,)
        ).fetchall()
        return [{"id": r[0], "name": r[1], "telegram_id": r[2]} for r in rows]
    finally:
        conn.close()


def get_all_contacts() -> list[dict]:
    """Получить все контакты."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT id, name, telegram_id, role FROM contacts ORDER BY role, name"
        ).fetchall()
        return [{"id": r[0], "name": r[1], "telegram_id": r[2], "role": r[3]} for r in rows]
    finally:
        conn.close()


def delete_contact(contact_id: int):
    """Удалить контакт по ID."""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM contacts WHERE id = ?", (contact_id,))
        conn.commit()
    finally:
        conn.close()


def set_status_override(phone: str, sheet_row: int, status: str):
    """Сохранить переопределение статуса."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO status_overrides (phone, sheet_row, status) VALUES (?, ?, ?)",
            (phone, sheet_row, status)
        )
        conn.commit()
    finally:
        conn.close()


def get_status_overrides() -> dict[tuple[str, int], str]:
    """Получить все переопределения статусов: (phone, row) -> status."""
    conn = get_connection()
    try:
        rows = conn.execute("SELECT phone, sheet_row, status FROM status_overrides").fetchall()
        return {(r[0], r[1]): r[2] for r in rows}
    finally:
        conn.close()


def set_econom_number(phone: str, sheet_row: int, econom_number: Optional[str]):
    """Сохранить номер эконом для строки."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO row_extras (phone, sheet_row, econom_number) VALUES (?, ?, ?) "
            "ON CONFLICT(phone, sheet_row) DO UPDATE SET econom_number = ?",
            (phone, sheet_row, econom_number or "", econom_number or "")
        )
        conn.commit()
    finally:
        conn.close()


def set_local_status(phone: str, sheet_row: int, local_status: str):
    """Сохранить локальный статус для строки."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO row_extras (phone, sheet_row, local_status) VALUES (?, ?, ?) "
            "ON CONFLICT(phone, sheet_row) DO UPDATE SET local_status = ?",
            (phone, sheet_row, local_status, local_status)
        )
        conn.commit()
    finally:
        conn.close()


def get_row_extras() -> dict[tuple[str, int], dict]:
    """Получить доп. данные: (phone, row) -> {econom_number, local_status, object_*}."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT phone, sheet_row, econom_number, local_status, object_address, object_area, object_budget, object_work_type FROM row_extras"
        ).fetchall()
        return {(r[0], r[1]): {
            "econom_number": r[2] or "",
            "local_status": r[3] or "first_contact",
            "object_address": r[4] or "",
            "object_area": r[5] or "",
            "object_budget": r[6] or "",
            "object_work_type": r[7] or "",
        } for r in rows}
    finally:
        conn.close()


def set_object_info(phone: str, sheet_row: int, address: str, area: str, budget: str, work_type: str):
    """Сохранить информацию об объекте."""
    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO row_extras (phone, sheet_row, object_address, object_area, object_budget, object_work_type)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(phone, sheet_row) DO UPDATE SET
                 object_address=?, object_area=?, object_budget=?, object_work_type=?""",
            (phone, sheet_row, address, area, budget, work_type, address, area, budget, work_type)
        )
        conn.commit()
    finally:
        conn.close()


def add_event(phone: str, event_type: str, description: str, sheet_row: Optional[int] = None):
    """Записать событие в лог клиента."""
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO events (phone, sheet_row, event_type, description) VALUES (?, ?, ?, ?)",
            (phone, sheet_row, event_type, description)
        )
        conn.commit()
    finally:
        conn.close()


def get_events(phone: str, limit: int = 50) -> list[dict]:
    """Получить историю событий по клиенту (новые сверху)."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT id, event_type, description, created_at FROM events WHERE phone = ? ORDER BY created_at DESC LIMIT ?",
            (phone, limit)
        ).fetchall()
        return [{"id": r[0], "type": r[1], "description": r[2], "created_at": r[3]} for r in rows]
    finally:
        conn.close()


def delete_event(event_id: int) -> bool:
    """Удалить событие по id. Возвращает True если удалено."""
    conn = get_connection()
    try:
        cur = conn.execute("DELETE FROM events WHERE id = ?", (event_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


STAGE_RANK = {"first_contact": 0, "negotiation": 1, "waiting": 2, "proposal_sent": 3, "closed": 4}


def get_max_stages() -> dict[tuple[str, int], int]:
    """Для каждой строки (phone, sheet_row) — максимальный этап воронки, которого она когда-либо достигала."""
    conn = get_connection()
    try:
        result: dict[tuple[str, int], int] = {}
        status_map = {
            "Установлен первый контакт": "first_contact",
            "Переговоры": "negotiation",
            "Ушли в долгое ожидание": "waiting",
            "Выслано КП": "proposal_sent",
            "Закрыт": "closed",
        }
        rows = conn.execute(
            "SELECT phone, sheet_row, description FROM events WHERE event_type = 'status_change'"
        ).fetchall()
        for phone, sheet_row, desc in rows:
            if sheet_row is None:
                continue
            key = (phone, sheet_row)
            for label, stage_key in status_map.items():
                if label in desc:
                    rank = STAGE_RANK.get(stage_key, 0)
                    result[key] = max(result.get(key, 0), rank)
                    break
        extras_rows = conn.execute(
            "SELECT phone, sheet_row, local_status FROM row_extras WHERE local_status IS NOT NULL AND local_status != ''"
        ).fetchall()
        for phone, sheet_row, local_status in extras_rows:
            key = (phone, sheet_row)
            rank = STAGE_RANK.get(local_status, 0)
            if local_status not in ("rejected", "low_interest", "contractors"):
                result[key] = max(result.get(key, 0), rank)
        return result
    finally:
        conn.close()


def save_task_message(
    tg_message_id: int,
    tg_chat_id: str,
    phone: str,
    role: str,
    task_text: str,
    parent_task_id: Optional[int] = None,
) -> Optional[int]:
    """Сохранить связку Telegram message_id -> клиент. parent_task_id — если задача от сметчицы/инженера другому."""
    conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO task_messages (tg_message_id, tg_chat_id, phone, role, task_text, parent_task_id) VALUES (?, ?, ?, ?, ?, ?)",
            (tg_message_id, tg_chat_id, phone, role, task_text, parent_task_id),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def find_task_by_message(tg_message_id: int, tg_chat_id: str) -> Optional[dict]:
    """Найти задачу по message_id и chat_id (для обработки ответов)."""
    conn = get_connection()
    try:
        row = conn.execute(
            """SELECT tm.id, tm.phone, tm.role, tm.task_text, tm.tg_chat_id,
                      (SELECT econom_number FROM row_extras WHERE phone = tm.phone LIMIT 1),
                      tm.parent_task_id,
                      (SELECT tg_chat_id FROM task_messages WHERE id = tm.parent_task_id)
               FROM task_messages tm WHERE tm.tg_message_id = ? AND tm.tg_chat_id = ?""",
            (tg_message_id, tg_chat_id),
        ).fetchone()
        if row:
            return {
                "id": row[0], "phone": row[1], "role": row[2], "task_text": row[3],
                "tg_chat_id": row[4], "client_name": row[5] or "", "parent_task_id": row[6], "reply_to_chat_id": row[7],
            }
        return None
    finally:
        conn.close()


def find_last_task_for_chat(tg_chat_id: str) -> Optional[dict]:
    """Найти последнюю задачу, отправленную в этот чат."""
    conn = get_connection()
    try:
        row = conn.execute(
            """SELECT tm.id, tm.phone, tm.role, tm.task_text, tm.tg_chat_id,
                      (SELECT econom_number FROM row_extras WHERE phone = tm.phone LIMIT 1),
                      tm.parent_task_id,
                      (SELECT tg_chat_id FROM task_messages WHERE id = tm.parent_task_id)
               FROM task_messages tm WHERE tm.tg_chat_id = ? ORDER BY tm.created_at DESC LIMIT 1""",
            (tg_chat_id,),
        ).fetchone()
        if row:
            return {
                "id": row[0], "phone": row[1], "role": row[2], "task_text": row[3],
                "tg_chat_id": row[4], "client_name": row[5] or "", "parent_task_id": row[6], "reply_to_chat_id": row[7],
            }
        return None
    finally:
        conn.close()


def get_all_tasks_for_manager() -> list[dict]:
    """Все задачи для раздела управления: кому, клиент, задача, статус."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT tm.id, tm.phone, tm.task_text, tm.created_at, tm.role, tm.status,
                      c.name,
                      (SELECT econom_number FROM row_extras WHERE phone = tm.phone LIMIT 1)
               FROM task_messages tm
               LEFT JOIN contacts c ON c.telegram_id = tm.tg_chat_id
               ORDER BY tm.created_at DESC
               LIMIT 100"""
        ).fetchall()
        return [{
            "id": r[0], "phone": r[1], "task_text": r[2], "created_at": r[3],
            "role": r[4], "status": r[5] or "new",
            "recipient_name": r[6] or "—", "client_name": r[7] or "",
        } for r in rows]
    finally:
        conn.close()


def get_tasks_for_user(tg_chat_id: str) -> list[dict]:
    """Получить все задачи, отправленные конкретному сотруднику."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT tm.id, tm.phone, tm.task_text, tm.created_at, tm.role,
                      re.econom_number, re.local_status, tm.status
               FROM task_messages tm
               LEFT JOIN row_extras re ON re.phone = tm.phone
               WHERE tm.tg_chat_id = ?
               ORDER BY tm.created_at DESC
               LIMIT 50""",
            (tg_chat_id,)
        ).fetchall()
        return [{
            "id": r[0], "phone": r[1], "task_text": r[2], "created_at": r[3],
            "role": r[4], "client_name": r[5] or "", "client_status": r[6] or "",
            "status": r[7] or "new",
        } for r in rows]
    finally:
        conn.close()


def delete_task(task_id: int) -> bool:
    """Удалить задачу по id. Возвращает True если удалено."""
    conn = get_connection()
    try:
        cur = conn.execute("DELETE FROM task_messages WHERE id = ?", (task_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def update_task_status(task_id: int, status: str):
    """Обновить статус задачи (new, in_progress, done)."""
    conn = get_connection()
    try:
        conn.execute("UPDATE task_messages SET status = ? WHERE id = ?", (status, task_id))
        conn.commit()
    finally:
        conn.close()


def get_task_by_id(task_id: int) -> Optional[dict]:
    """Получить задачу по ID."""
    conn = get_connection()
    try:
        row = conn.execute(
            """SELECT tm.id, tm.phone, tm.task_text, tm.tg_chat_id, tm.role, tm.status,
                      (SELECT econom_number FROM row_extras WHERE phone = tm.phone LIMIT 1),
                      tm.parent_task_id,
                      (SELECT tg_chat_id FROM task_messages WHERE id = tm.parent_task_id)
               FROM task_messages tm WHERE tm.id = ?""",
            (task_id,),
        ).fetchone()
        if row:
            return {
                "id": row[0], "phone": row[1], "task_text": row[2],
                "tg_chat_id": row[3], "role": row[4], "status": row[5] or "new",
                "client_name": row[6] or "", "parent_task_id": row[7], "reply_to_chat_id": row[8],
            }
        return None
    finally:
        conn.close()


def get_unfinished_tasks_for_reminder(hours_since_last: int = 24) -> list[dict]:
    """Задачи со статусом != done, которым можно отправить напоминание (не чаще раз в N часов)."""
    conn = get_connection()
    try:
        cutoff = (datetime.now() - timedelta(hours=hours_since_last)).strftime("%Y-%m-%d %H:%M:%S")
        rows = conn.execute(
            """SELECT tm.id, tm.phone, tm.task_text, tm.tg_chat_id, tm.role, tm.created_at,
                      re.econom_number
               FROM task_messages tm
               LEFT JOIN row_extras re ON re.phone = tm.phone
               WHERE tm.status != 'done' AND tm.tg_chat_id IS NOT NULL AND tm.tg_chat_id != ''
                 AND (tm.last_reminder_at IS NULL OR tm.last_reminder_at <= ?)
               ORDER BY tm.created_at ASC""",
            (cutoff,),
        ).fetchall()
        return [{
            "id": r[0], "phone": r[1], "task_text": r[2], "tg_chat_id": r[3],
            "role": r[4], "created_at": r[5], "client_name": r[6] or "",
        } for r in rows]
    finally:
        conn.close()


def record_task_reminder_sent(task_id: int):
    """Записать время отправки напоминания по задаче."""
    conn = get_connection()
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("UPDATE task_messages SET last_reminder_at = ? WHERE id = ?", (now, task_id))
        conn.commit()
    finally:
        conn.close()


def get_client_full_history(phone: str) -> list[dict]:
    """Полная история клиента: события + задачи + комментарии, единая лента."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT 'event' as src, event_type as type, description, created_at
               FROM events WHERE phone = ?
               UNION ALL
               SELECT 'task', 'task_assigned', task_text, created_at
               FROM task_messages WHERE phone = ?
               ORDER BY created_at DESC
               LIMIT 100""",
            (phone, phone)
        ).fetchall()
        return [{"src": r[0], "type": r[1], "description": r[2], "created_at": r[3]} for r in rows]
    finally:
        conn.close()


def get_last_activity_by_row() -> dict[tuple[str, int], str]:
    """Для каждой (phone, sheet_row) — дата последнего события (created_at)."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT phone, sheet_row, MAX(created_at) as last_activity
               FROM events WHERE sheet_row IS NOT NULL
               GROUP BY phone, sheet_row"""
        ).fetchall()
        return {(r[0], r[1]): r[2] for r in rows if r[2]}
    finally:
        conn.close()


def get_inactive_clients(days: int = 3) -> list[dict]:
    """Получить клиентов без активности N+ дней (кроме закрытых/отказов)."""
    conn = get_connection()
    try:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        rows = conn.execute("""
            SELECT re.phone, re.sheet_row, re.econom_number, re.local_status,
                   MAX(e.created_at) as last_activity
            FROM row_extras re
            LEFT JOIN events e ON e.phone = re.phone
            WHERE re.local_status NOT IN ('closed', 'rejected', 'low_interest')
            GROUP BY re.phone, re.sheet_row
            HAVING last_activity IS NULL OR last_activity < ?
        """, (cutoff,)).fetchall()
        return [{"phone": r[0], "sheet_row": r[1], "name": r[2] or "", "status": r[3], "last_activity": r[4]} for r in rows]
    finally:
        conn.close()


LEGAL_LEAD_STATUSES = ("pool", "outreach", "qualified", "stop")


def _legal_touch_updated(conn, lead_id: int):
    conn.execute(
        "UPDATE legal_leads SET updated_at = ? WHERE id = ?",
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), lead_id),
    )
    conn.commit()


def _legal_row_from_sql(r: tuple) -> dict:
    return {
        "id": r[0],
        "company_name": r[1] or "",
        "inn": r[2] or "",
        "phone": r[3] or "",
        "email": r[4] or "",
        "okved": r[5] or "",
        "region": r[6] or "",
        "status": r[7] or "pool",
        "notes": r[8] or "",
        "source": r[9] or "",
        "next_contact_at": r[10] or "",
        "priority": int(r[11] or 0),
        "created_at": r[12],
        "updated_at": r[13],
    }


def legal_leads_list(
    status_filter: Optional[str] = None,
    due_only: bool = False,
) -> list[dict]:
    conn = get_connection()
    try:
        q = """SELECT id, company_name, inn, phone, email, okved, region, status, notes, source,
                      next_contact_at, priority, created_at, updated_at FROM legal_leads"""
        params: tuple = ()
        if status_filter and status_filter in LEGAL_LEAD_STATUSES:
            q += " WHERE status = ?"
            params = (status_filter,)
        q += " ORDER BY priority DESC, updated_at DESC"
        rows = conn.execute(q, params).fetchall()
        out = [_legal_row_from_sql(r) for r in rows]
    finally:
        conn.close()
    if due_only:
        today = datetime.now().strftime("%Y-%m-%d")
        out = [
            r
            for r in out
            if r["status"] != "stop"
            and r.get("next_contact_at")
            and (r["next_contact_at"][:10] <= today)
        ]
    return out


def legal_lead_find_id_by_inn(inn: str) -> Optional[int]:
    if not inn or not str(inn).strip():
        return None
    conn = get_connection()
    try:
        row = conn.execute("SELECT id FROM legal_leads WHERE inn = ?", (str(inn).strip(),)).fetchone()
        return int(row[0]) if row else None
    finally:
        conn.close()


def legal_lead_summary() -> dict[str, int]:
    conn = get_connection()
    try:
        rows = conn.execute("SELECT status, COUNT(*) FROM legal_leads GROUP BY status").fetchall()
        out = {s: 0 for s in LEGAL_LEAD_STATUSES}
        for st, cnt in rows:
            if st in out:
                out[st] = cnt
        total_row = conn.execute("SELECT COUNT(*) FROM legal_leads").fetchone()
        out["total"] = int(total_row[0]) if total_row else sum(out.values())
        return out
    finally:
        conn.close()


def legal_lead_create(
    company_name: str,
    inn: str = "",
    phone: str = "",
    email: str = "",
    okved: str = "",
    region: str = "",
    source: str = "manual",
    status: str = "pool",
    next_contact_at: str = "",
    priority: int = 0,
) -> int:
    if status not in LEGAL_LEAD_STATUSES:
        status = "pool"
    p = max(0, min(2, int(priority)))
    conn = get_connection()
    try:
        cur = conn.execute(
            """INSERT INTO legal_leads (company_name, inn, phone, email, okved, region, status, source, next_contact_at, priority)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                company_name.strip(),
                inn.strip(),
                phone.strip(),
                email.strip(),
                okved.strip(),
                region.strip(),
                status,
                source,
                (next_contact_at or "").strip(),
                p,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def legal_lead_get(lead_id: int) -> Optional[dict]:
    conn = get_connection()
    try:
        r = conn.execute(
            """SELECT id, company_name, inn, phone, email, okved, region, status, notes, source,
                      next_contact_at, priority, created_at, updated_at
               FROM legal_leads WHERE id = ?""",
            (lead_id,),
        ).fetchone()
        if not r:
            return None
        return _legal_row_from_sql(r)
    finally:
        conn.close()


def legal_lead_update(
    lead_id: int,
    company_name: Optional[str] = None,
    inn: Optional[str] = None,
    phone: Optional[str] = None,
    email: Optional[str] = None,
    okved: Optional[str] = None,
    region: Optional[str] = None,
    status: Optional[str] = None,
    notes: Optional[str] = None,
    next_contact_at: Optional[str] = None,
    priority: Optional[int] = None,
) -> bool:
    row = legal_lead_get(lead_id)
    if not row:
        return False
    new_status = row["status"] if status is None else status
    if new_status not in LEGAL_LEAD_STATUSES:
        new_status = row["status"]
    nc = row["next_contact_at"] if next_contact_at is None else (next_contact_at or "").strip()
    pr = row["priority"] if priority is None else max(0, min(2, int(priority)))
    conn = get_connection()
    try:
        conn.execute(
            """UPDATE legal_leads SET
               company_name = ?, inn = ?, phone = ?, email = ?, okved = ?, region = ?,
               status = ?, notes = ?, next_contact_at = ?, priority = ?,
               updated_at = ?
               WHERE id = ?""",
            (
                (company_name if company_name is not None else row["company_name"]).strip(),
                (inn if inn is not None else row["inn"]).strip(),
                (phone if phone is not None else row["phone"]).strip(),
                (email if email is not None else row["email"]).strip(),
                (okved if okved is not None else row["okved"]).strip(),
                (region if region is not None else row["region"]).strip(),
                new_status,
                (notes if notes is not None else row["notes"]).strip(),
                nc,
                pr,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                lead_id,
            ),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def legal_lead_add_event(lead_id: int, description: str, event_type: str = "note") -> bool:
    if not legal_lead_get(lead_id):
        return False
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO legal_lead_events (lead_id, event_type, description) VALUES (?, ?, ?)",
            (lead_id, event_type, description.strip()),
        )
        _legal_touch_updated(conn, lead_id)
        return True
    finally:
        conn.close()


def legal_lead_events(lead_id: int, limit: int = 50) -> list[dict]:
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT id, event_type, description, created_at FROM legal_lead_events
               WHERE lead_id = ? ORDER BY created_at DESC LIMIT ?""",
            (lead_id, limit),
        ).fetchall()
        return [{"id": r[0], "type": r[1], "description": r[2], "created_at": r[3]} for r in rows]
    finally:
        conn.close()


def _nz(s: Optional[str]) -> Optional[str]:
    t = (s or "").strip()
    return t if t else None


def legal_import_upsert_row(
    company_name: str,
    inn: str = "",
    phone: str = "",
    email: str = "",
    okved: str = "",
    region: str = "",
    source: str = "import",
    next_contact_at: str = "",
    priority: int = 0,
) -> tuple[str, int]:
    """created | updated | skipped, lead_id (-1 если skipped)."""
    cn = (company_name or "").strip()
    if not cn:
        return "skipped", -1
    inn = (inn or "").strip()
    pr = max(0, min(2, int(priority or 0)))
    nc_raw = (next_contact_at or "").strip()
    eid = legal_lead_find_id_by_inn(inn) if inn else None
    if eid:
        old = legal_lead_get(eid)
        if not old:
            return "skipped", -1
        legal_lead_update(
            eid,
            company_name=cn,
            inn=inn or old["inn"],
            phone=_nz(phone),
            email=_nz(email),
            okved=_nz(okved),
            region=_nz(region),
            next_contact_at=nc_raw if nc_raw else None,
            priority=pr,
        )
        legal_lead_add_event(eid, f"Импорт: обновление по ИНН (источник {source})", "system")
        return "updated", eid
    nid = legal_lead_create(
        cn,
        inn,
        phone or "",
        email or "",
        okved or "",
        region or "",
        source,
        "pool",
        nc_raw,
        pr,
    )
    legal_lead_add_event(nid, f"Импорт ({source})", "system")
    return "created", nid
