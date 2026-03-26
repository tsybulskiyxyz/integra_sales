"""Нормализация строк юр-лидов из Google Sheets (первая строка = заголовки)."""

from typing import Any

LEGAL_SHEET_FIELD_ALIASES = {
    "company_name": ("company_name", "название", "компания", "наименование", "name", "организация", "org", "полное_наименование"),
    "inn": ("inn", "инн", "инн/кпп"),
    "phone": ("phone", "телефон", "tel", "тел", "мобильный"),
    "email": ("email", "e-mail", "почта", "mail"),
    "okved": ("okved", "оквэд", "оквед"),
    "region": ("region", "регион", "город", "адрес"),
    "next_contact_at": ("next_contact_at", "next_contact", "следующее_касание", "дата_касания", "follow_up", "followup"),
    "priority": ("priority", "приоритет"),
    "notes": ("notes", "комментарий", "примечание", "comment"),
}


def normalize_legal_header(h: str) -> str:
    return (h or "").strip().lower().replace("\ufeff", "").replace("ё", "е").replace(" ", "_")


def pick_legal_field(rev: dict[str, str], field: str) -> str:
    for alias in LEGAL_SHEET_FIELD_ALIASES[field]:
        if alias in rev:
            return (rev[alias] or "").strip()
    return ""


def parse_legal_priority(val: str) -> int:
    t = (val or "").strip().lower()
    if t in ("2", "срочно", "urgent"):
        return 2
    if t in ("1", "высокий", "high"):
        return 1
    try:
        return max(0, min(2, int(float(t))))
    except ValueError:
        return 0


def legal_row_from_sheet_rev(rev: dict[str, str]) -> dict[str, Any]:
    return {
        "company_name": pick_legal_field(rev, "company_name"),
        "inn": pick_legal_field(rev, "inn"),
        "phone": pick_legal_field(rev, "phone"),
        "email": pick_legal_field(rev, "email"),
        "okved": pick_legal_field(rev, "okved"),
        "region": pick_legal_field(rev, "region"),
        "next_contact_at": pick_legal_field(rev, "next_contact_at"),
        "priority": parse_legal_priority(pick_legal_field(rev, "priority")),
        "notes": pick_legal_field(rev, "notes"),
    }
