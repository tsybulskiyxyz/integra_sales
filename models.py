"""Модели данных."""
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class RowStatus(str, Enum):
    """Статус строки по цвету."""
    RED = "red"           # услуга не нужна
    GREEN = "green"       # целевой, переговоры
    ORANGE = "orange"     # не дозвонился
    PURPLE = "purple"     # закрытая сделка
    UNKNOWN = "unknown"


@dataclass
class CallRow:
    """Строка из таблицы звонков."""
    row_index: int
    phone: str
    name: str
    status: RowStatus
    result: str
    comments: str
    creation_time: str
    call_duration: str
    conversation_link: str
    day_index: int  # индекс рабочего дня (по пробелам)


@dataclass
class Stats:
    """Статистика по звонкам."""
    total_rows: int
    red_count: int
    green_count: int
    orange_count: int
    purple_count: int
    reached_count: int      # дозвоны = red + green + purple
    target_percent: float   # зелёные / дозвоны * 100
    closed_percent: float   # фиолетовые / зелёные * 100 (от целевых)
    working_days: int
