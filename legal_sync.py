"""Нормализация строк юр-лидов из Google Sheets (первая строка = заголовки)."""

import re
from typing import Any

LEGAL_SHEET_FIELD_ALIASES = {
    "company_name": (
        "company_name",
        "название",
        "компания",
        "наименование",
        "name",
        "организация",
        "org",
        "полное_наименование",
        "контрагент",
        "наименование_контрагента",
        "краткое_наименование",
        "заказчик",
        "клиент",
        "юрлицо",
        "юр_лицо",
        "юридическое_лицо",
        "поставщик",
        "subject",
        "title",
    ),
    "inn": ("inn", "инн", "инн/кпп"),
    "phone": (
        "phone",
        "телефон",
        "tel",
        "тел",
        "мобильный",
        "телефоны",
        "phones",
        "номера",
        "номер",
        "список_телефонов",
        "phone_list",
        "доп_телефон",
        "дополнительный_телефон",
    ),
    "email": (
        "email",
        "e-mail",
        "почта",
        "mail",
        "emails",
        "почты",
        "список_email",
        "email_list",
        "адреса_email",
    ),
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


def _split_phone_tokens(*text_parts: str) -> list[str]:
    """Несколько номеров: в ячейке через запятую, ; слэш, перенос; внутри номера — пробелы и дефисы."""
    out: list[str] = []
    seen: set[str] = set()
    for raw in text_parts:
        if not raw or not str(raw).strip():
            continue
        for segment in re.split(r"[,;/|]+|\n+", str(raw).strip()):
            segment = segment.strip()
            if not segment:
                continue
            digits = re.sub(r"\D", "", segment)
            if len(digits) >= 10:
                if digits not in seen:
                    seen.add(digits)
                    out.append(digits)
    return out


def _split_email_tokens(*text_parts: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    email_re = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
    for raw in text_parts:
        if not raw or not str(raw).strip():
            continue
        for part in re.split(r"[\s,;|/\n]+", str(raw).strip()):
            part = part.strip().lower()
            if not part:
                continue
            m = email_re.search(part)
            if m:
                e = m.group(0).lower()
                if e not in seen:
                    seen.add(e)
                    out.append(e)
    return out


def collect_phones_from_rev(rev: dict[str, str]) -> str:
    """Все ячейки с телефонами: явные алиасы + заголовки с «телефон»/«phone» (без email)."""
    parts: list[str] = []
    used_keys: set[str] = set()
    for alias in LEGAL_SHEET_FIELD_ALIASES["phone"]:
        if alias in rev and (rev[alias] or "").strip():
            parts.append((rev[alias] or "").strip())
            used_keys.add(alias)
    for k, v in rev.items():
        if not k or not (v or "").strip():
            continue
        kl = k.lower()
        if k in used_keys:
            continue
        if "email" in kl and "тел" not in kl:
            continue
        if "телефон" in kl or kl in ("tel", "phones", "mobile", "моб", "мобильный"):
            parts.append(v.strip())
        elif kl == "phone" or kl.startswith("phone_") or kl.endswith("_phone"):
            parts.append(v.strip())
    nums = _split_phone_tokens(*parts)
    return ", ".join(nums) if nums else ""


def collect_emails_from_rev(rev: dict[str, str]) -> str:
    parts: list[str] = []
    used_keys: set[str] = set()
    for alias in LEGAL_SHEET_FIELD_ALIASES["email"]:
        if alias in rev and (rev[alias] or "").strip():
            parts.append((rev[alias] or "").strip())
            used_keys.add(alias)
    for k, v in rev.items():
        if not k or not (v or "").strip():
            continue
        kl = k.lower()
        if k in used_keys:
            continue
        if "почт" in kl or "email" in kl or kl in ("mail", "e-mail", "mails"):
            parts.append(v.strip())
    emails = _split_email_tokens(*parts)
    return ", ".join(emails) if emails else ""


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
    inn = pick_legal_field(rev, "inn")
    phone = collect_phones_from_rev(rev)
    company_name = (pick_legal_field(rev, "company_name") or "").strip()
    if not company_name:
        if inn:
            company_name = f"Компания ИНН {inn}"
        elif phone:
            first = phone.split(",")[0].strip()
            company_name = f"Компания (тел. {first})"
        else:
            # колонка с названием под нестандартным заголовком — берём первую «текстовую» ячейку
            skip_keys = {
                "inn",
                "phone",
                "email",
                "okved",
                "region",
                "next_contact_at",
                "priority",
                "notes",
                *LEGAL_SHEET_FIELD_ALIASES["inn"],
                *LEGAL_SHEET_FIELD_ALIASES["phone"],
                *LEGAL_SHEET_FIELD_ALIASES["email"],
            }
            for k, v in rev.items():
                kk = (k or "").strip().lower()
                if kk in skip_keys or not kk:
                    continue
                t = (v or "").strip()
                if len(t) >= 2 and not re.fullmatch(r"[\d\s\-+()]+", t):
                    company_name = t
                    break
    return {
        "company_name": company_name,
        "inn": inn,
        "phone": phone,
        "email": collect_emails_from_rev(rev),
        "okved": pick_legal_field(rev, "okved"),
        "region": pick_legal_field(rev, "region"),
        "next_contact_at": pick_legal_field(rev, "next_contact_at"),
        "priority": parse_legal_priority(pick_legal_field(rev, "priority")),
        "notes": pick_legal_field(rev, "notes"),
    }
