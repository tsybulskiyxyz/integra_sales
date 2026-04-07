"""Интеграция с Google Sheets."""
import re
import json
from typing import Optional
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from config import GOOGLE_CREDENTIALS_PATH, GOOGLE_SHEET_URL
from legal_sync import legal_row_from_sheet_rev, normalize_legal_header
from models import CallRow, RowStatus


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


def _rgb_to_status(bg: dict) -> RowStatus:
    """Определяет статус по backgroundColor dict из API (значения 0-1).
    Стандартные цвета Google Sheets:
      Red:    {red:1}                  → (255, 0, 0)
      Green:  {green:1}               → (0, 255, 0)
      Orange: {red:1, green:0.6}      → (255, 153, 0)
      Purple: {red:0.6, blue:1}       → (153, 0, 255)
      White:  {red:1, green:1, blue:1} или {}
    """
    r = float(bg.get("red", 0))
    g = float(bg.get("green", 0))
    b = float(bg.get("blue", 0))

    rn, gn, bn = int(r * 255), int(g * 255), int(b * 255)

    if rn > 240 and gn > 240 and bn > 240:
        return RowStatus.UNKNOWN

    # Фиолетовый: B доминирует, R средний, G низкий
    if bn > 100 and bn >= rn and bn > gn and gn < 180:
        return RowStatus.PURPLE

    # Зелёный: G доминирует
    if gn > 100 and gn > rn and gn > bn:
        return RowStatus.GREEN

    # Оранжевый: R высокий, G средний, B низкий
    if rn > 200 and 80 < gn < 230 and bn < 100:
        return RowStatus.ORANGE

    # Красный: R доминирует, G и B низкие
    if rn > 150 and rn > gn and rn > bn:
        return RowStatus.RED

    return RowStatus.UNKNOWN


def extract_sheet_id(url: str) -> Optional[str]:
    match = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
    return match.group(1) if match else None


def _get_sheets_service():
    creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_PATH, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def get_spreadsheet_last_modified(sheet_url: Optional[str] = None) -> Optional[str]:
    """Дата последнего изменения таблицы (Drive API). Формат: DD.MM.YYYY HH:MM."""
    url = sheet_url or GOOGLE_SHEET_URL
    if not url:
        return None
    sheet_id = extract_sheet_id(url)
    if not sheet_id:
        return None
    try:
        creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_PATH, scopes=SCOPES)
        drive = build("drive", "v3", credentials=creds)
        meta = drive.files().get(fileId=sheet_id, fields="modifiedTime").execute()
        mt = meta.get("modifiedTime")
        if mt:
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(mt.replace("Z", "+00:00"))
            local = dt.astimezone()
            return local.strftime("%d.%m.%Y %H:%M")
    except Exception:
        pass
    return None


def _get_row_color(row_data: dict) -> RowStatus:
    """Определяет цвет строки по первой ячейке с заданным цветом фона."""
    for cell in row_data.get("values", []):
        for key in ("userEnteredFormat", "effectiveFormat"):
            fmt = cell.get(key)
            if not fmt:
                continue
            bg = fmt.get("backgroundColor") or fmt.get("backgroundColorStyle", {}).get("rgbColor")
            if bg and isinstance(bg, dict):
                status = _rgb_to_status(bg)
                if status != RowStatus.UNKNOWN:
                    return status
    return RowStatus.UNKNOWN


def fetch_call_data(sheet_url: Optional[str] = None) -> tuple[list[CallRow], int]:
    url = sheet_url or GOOGLE_SHEET_URL
    if not url:
        raise ValueError("Укажите GOOGLE_SHEET_URL в .env")

    sheet_id = extract_sheet_id(url)
    if not sheet_id:
        raise ValueError("Некорректная ссылка на Google таблицу")

    service = _get_sheets_service()

    meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheet_name = meta["sheets"][0]["properties"]["title"]

    # 1) Значения
    values_result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"'{sheet_name}'!A:Z"
    ).execute()
    data = values_result.get("values", [])

    # 2) Цвета — запрашиваем ТОЛЬКО backgroundColor через fields
    row_colors: list[dict] = []
    try:
        color_result = service.spreadsheets().get(
            spreadsheetId=sheet_id,
            ranges=[f"'{sheet_name}'!A1:Z{len(data) + 5}"],
            fields="sheets.data.rowData.values.userEnteredFormat.backgroundColor",
            includeGridData=True
        ).execute()
        sheets = color_result.get("sheets", [])
        if sheets:
            grid_data = sheets[0].get("data", [])
            if grid_data:
                row_colors = grid_data[0].get("rowData", [])
    except Exception as e:
        print(f"[WARN] Не удалось получить цвета: {e}")

    # Рабочие дни = количество строк где столбец A = "Call id"
    header_rows = set()
    working_days = 0
    for idx, row in enumerate(data):
        if row and "call id" in str(row[0]).strip().lower():
            working_days += 1
            header_rows.add(idx)

    # day_index: к какому рабочему дню относится строка (0, 1, 2... — по порядку "Call id")
    sorted_headers = sorted(header_rows)
    rows: list[CallRow] = []
    for i, row in enumerate(data):
        if i in header_rows:
            continue

        if not any(str(c).strip() for c in row[:5]):
            continue

        phone = str(row[2]).strip() if len(row) > 2 else ""
        digits = re.sub(r"\D", "", phone)
        if not digits or len(digits) < 10:
            continue

        creation_time = str(row[4]).strip() if len(row) > 4 else ""
        result = str(row[11]).strip() if len(row) > 11 else ""
        comments = str(row[8]).strip() if len(row) > 8 else ""

        # День = сколько заголовков "Call id" перед этой строкой (0 = первый день, 1 = второй...)
        day_index = len([h for h in sorted_headers if h < i]) - 1
        if day_index < 0:
            day_index = 0

        # Статус ТОЛЬКО по цвету
        status = RowStatus.UNKNOWN
        if i < len(row_colors):
            status = _get_row_color(row_colors[i])

        rows.append(CallRow(
            row_index=i + 1,
            phone=phone,
            name="",
            status=status,
            result=result,
            comments=comments,
            creation_time=creation_time,
            call_duration="",
            conversation_link="",
            day_index=day_index
        ))

    return rows, working_days


def debug_colors(sheet_url: Optional[str] = None, max_rows: int = 15) -> list[dict]:
    """Возвращает сырые RGB для первых N строк — для отладки."""
    url = sheet_url or GOOGLE_SHEET_URL
    sheet_id = extract_sheet_id(url)
    service = _get_sheets_service()
    meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheet_name = meta["sheets"][0]["properties"]["title"]

    result = service.spreadsheets().get(
        spreadsheetId=sheet_id,
        ranges=[f"'{sheet_name}'!A1:C{max_rows}"],
        fields="sheets.data.rowData.values.userEnteredFormat.backgroundColor",
        includeGridData=True
    ).execute()

    out = []
    sheets = result.get("sheets", [])
    if sheets:
        grid = sheets[0].get("data", [{}])[0]
        for idx, rd in enumerate(grid.get("rowData", [])):
            cells = rd.get("values", [])
            if cells:
                bg = cells[0].get("userEnteredFormat", {}).get("backgroundColor")
                status = _rgb_to_status(bg) if bg else "no_color"
                out.append({"row": idx + 1, "bg": bg, "status": str(status)})
            else:
                out.append({"row": idx + 1, "bg": None, "status": "empty"})
    return out


def _legal_sheet_uses_physical_layout(data: list) -> bool:
    """Тот же лист, что у физиков: в столбце A встречается строка заголовка блока «Call id»."""
    for row in data[:800]:
        if row and len(row) > 0 and "call id" in str(row[0]).strip().lower():
            return True
    return False


def _physical_block_header_row_indices(data: list) -> set[int]:
    return {i for i, row in enumerate(data) if row and "call id" in str(row[0]).strip().lower()}


def _legal_row_dict_from_physical_row(row: list) -> Optional[dict]:
    """Колонки как в fetch_call_data: телефон в C (индекс 2), комментарий в I (8)."""
    if not any(str(c).strip() for c in row[:5]):
        return None
    phone = str(row[2]).strip() if len(row) > 2 else ""
    digits = re.sub(r"\D", "", phone)
    if not digits or len(digits) < 10:
        return None
    comments = str(row[8]).strip() if len(row) > 8 else ""
    return {
        "company_name": "",
        "inn": "",
        "phone": phone,
        "email": "",
        "okved": "",
        "region": "",
        "next_contact_at": "",
        "priority": 0,
        "notes": comments,
    }


def fetch_legal_sheet_rows(sheet_url: str) -> list[dict]:
    """
    Два формата: (1) как у физиков — блоки с «Call id», телефон в колонке C; (2) первая строка — заголовки.
    Строка попадает в выдачу, если есть название и/или нормальный телефон.
    """
    if not sheet_url:
        raise ValueError("Укажите ссылку на Google таблицу (юрики)")
    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        raise ValueError("Некорректная ссылка на Google таблицу")

    service = _get_sheets_service()
    meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheet_name = meta["sheets"][0]["properties"]["title"]

    values_result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"'{sheet_name}'!A:AZ",
    ).execute()
    data = values_result.get("values", [])
    if not data:
        return []

    if _legal_sheet_uses_physical_layout(data):
        out: list[dict] = []
        skip = _physical_block_header_row_indices(data)
        for i, row in enumerate(data):
            if i in skip:
                continue
            rec = _legal_row_dict_from_physical_row(row)
            if rec:
                out.append(rec)
        return out

    if len(data) < 2:
        return []

    headers = [normalize_legal_header(str(c)) for c in data[0]]
    out = []
    for row in data[1:]:
        if not any(str(c).strip() for c in row[:6] if c):
            continue
        cells = [str(row[i]).strip() if i < len(row) else "" for i in range(len(headers))]
        rev: dict[str, str] = {}
        for i, h in enumerate(headers):
            if not h or i >= len(cells):
                continue
            v = cells[i]
            if h not in rev:
                rev[h] = v
            else:
                if v:
                    if rev[h].strip():
                        rev[h] = rev[h].strip() + ", " + v
                    else:
                        rev[h] = v
        parsed = legal_row_from_sheet_rev(rev)
        cn = (parsed.get("company_name") or "").strip()
        ph = (parsed.get("phone") or "").strip()
        dig = re.sub(r"\D", "", ph.split(",")[0] if ph else "")
        if cn or len(dig) >= 10:
            out.append(parsed)
    return out


def fetch_legal_sheet_dashboard_rows(sheet_url: str) -> dict:
    """
    Строки таблицы юрлиц с цветом фона (как у физиков: зелёный / оранжевый / красный / фиолетовый).
    Используется для блока «Дозвонить» и сводки по цветам.
    """
    if not sheet_url:
        raise ValueError("Укажите ссылку на Google таблицу (юрики)")

    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        raise ValueError("Некорректная ссылка на Google таблицу")

    service = _get_sheets_service()
    meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheet_name = meta["sheets"][0]["properties"]["title"]

    values_result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"'{sheet_name}'!A:AZ",
    ).execute()
    data = values_result.get("values", [])
    if not data:
        return {
            "orange": [],
            "color_summary": {"green": 0, "orange": 0, "red": 0, "purple": 0, "unknown": 0},
            "total_rows": 0,
            "rows": [],
        }

    row_colors: list[dict] = []
    try:
        nrows = max(len(data) + 5, 10)
        color_result = service.spreadsheets().get(
            spreadsheetId=sheet_id,
            ranges=[f"'{sheet_name}'!A1:AZ{nrows}"],
            fields="sheets.data.rowData.values.userEnteredFormat.backgroundColor",
            includeGridData=True,
        ).execute()
        sheets = color_result.get("sheets", [])
        if sheets:
            grid_data = sheets[0].get("data", [])
            if grid_data:
                row_colors = grid_data[0].get("rowData", [])
    except Exception as e:
        print(f"[WARN] legal sheet colors: {e}")

    color_summary = {"green": 0, "orange": 0, "red": 0, "purple": 0, "unknown": 0}
    parsed_rows: list[dict] = []
    orange: list[dict] = []

    def _count_row(i: int, company: str, phone_raw: str, inn: str) -> None:
        status = RowStatus.UNKNOWN
        if i < len(row_colors):
            status = _get_row_color(row_colors[i])
        st_val = status.value
        if st_val in color_summary:
            color_summary[st_val] += 1
        else:
            color_summary["unknown"] += 1
        sheet_row = i + 1
        rec = {
            "sheet_row": sheet_row,
            "company_name": company,
            "phone": phone_raw,
            "inn": inn,
            "status": st_val,
        }
        parsed_rows.append(rec)
        digits = re.sub(r"\D", "", phone_raw.split(",")[0] if phone_raw else "")
        if st_val == RowStatus.ORANGE.value:
            orange.append(
                {
                    "phone": phone_raw or digits,
                    "row_index": sheet_row,
                    "creation_time": company,
                    "company_name": company,
                }
            )

    if _legal_sheet_uses_physical_layout(data):
        skip = _physical_block_header_row_indices(data)
        for i, row in enumerate(data):
            if i in skip:
                continue
            leg = _legal_row_dict_from_physical_row(row)
            if not leg:
                continue
            _count_row(i, "", leg["phone"], "")
    else:
        if len(data) < 2:
            return {
                "orange": [],
                "color_summary": {"green": 0, "orange": 0, "red": 0, "purple": 0, "unknown": 0},
                "total_rows": 0,
                "rows": [],
            }
        headers = [normalize_legal_header(str(c)) for c in data[0]]
        for i, row in enumerate(data):
            if i == 0:
                continue
            if not any(str(c).strip() for c in row[:8] if c):
                continue

            cells = [str(row[j]).strip() if j < len(row) else "" for j in range(len(headers))]
            rev: dict[str, str] = {}
            for j, h in enumerate(headers):
                if not h or j >= len(cells):
                    continue
                v = cells[j]
                if h not in rev:
                    rev[h] = v
                else:
                    if v:
                        if rev[h].strip():
                            rev[h] = rev[h].strip() + ", " + v
                        else:
                            rev[h] = v

            parsed = legal_row_from_sheet_rev(rev)
            company = (parsed.get("company_name") or "").strip()
            phone_raw = (parsed.get("phone") or "").strip()
            digits = re.sub(r"\D", "", phone_raw.split(",")[0] if phone_raw else "")
            if not company and len(digits) < 10:
                continue

            inn = (parsed.get("inn") or "").strip()
            _count_row(i, company, phone_raw, inn)

    return {
        "orange": orange,
        "color_summary": color_summary,
        "total_rows": len(parsed_rows),
        "rows": parsed_rows,
    }


def fetch_sheet_flat_text(sheet_url: str) -> str:
    """Все непустые ячейки первого листа в одну строку (для извлечения email и т.п.)."""
    if not sheet_url:
        raise ValueError("Укажите ссылку на Google Таблицу")
    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        raise ValueError("Некорректная ссылка на Google Таблицу")

    service = _get_sheets_service()
    meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheet_name = meta["sheets"][0]["properties"]["title"]

    values_result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"'{sheet_name}'!A:AZ",
    ).execute()
    rows = values_result.get("values", [])
    parts: list[str] = []
    for row in rows:
        for cell in row:
            if cell is not None and str(cell).strip():
                parts.append(str(cell).strip())
    return "\n".join(parts)
