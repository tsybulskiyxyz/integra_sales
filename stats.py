"""Расчёт статистики по звонкам."""
from models import CallRow, Stats, RowStatus


def _effective_status(r: CallRow, overrides: dict) -> RowStatus:
    key = (r.phone, r.row_index)
    s = overrides.get(key)
    if s:
        try:
            return RowStatus(s)
        except ValueError:
            pass
    return r.status


def calculate_stats(rows: list[CallRow], working_days: int) -> Stats:
    """Считает статистику."""
    red = sum(1 for r in rows if r.status == RowStatus.RED)
    green = sum(1 for r in rows if r.status == RowStatus.GREEN)
    orange = sum(1 for r in rows if r.status == RowStatus.ORANGE)
    purple = sum(1 for r in rows if r.status == RowStatus.PURPLE)

    # Дозвоны = красные + зелёные + фиолетовые (без оранжевых — не дозвонился)
    reached = red + green + purple

    target_percent = (green / reached * 100) if reached else 0.0
    closed_percent = (purple / green * 100) if green else 0.0

    return Stats(
        total_rows=len(rows),
        red_count=red,
        green_count=green,
        orange_count=orange,
        purple_count=purple,
        reached_count=reached,
        target_percent=round(target_percent, 1),
        closed_percent=round(closed_percent, 1),
        working_days=working_days
    )


def calculate_stats_with_overrides(rows: list[CallRow], overrides: dict, working_days: int = 0) -> Stats:
    """Считает статистику с учётом локальных переопределений статуса."""
    statuses = [_effective_status(r, overrides) for r in rows]
    red = sum(1 for s in statuses if s == RowStatus.RED)
    green = sum(1 for s in statuses if s == RowStatus.GREEN)
    orange = sum(1 for s in statuses if s == RowStatus.ORANGE)
    purple = sum(1 for s in statuses if s == RowStatus.PURPLE)
    reached = red + green + purple
    target_percent = (green / reached * 100) if reached else 0.0
    closed_percent = (purple / green * 100) if green else 0.0
    return Stats(
        total_rows=len(rows),
        red_count=red,
        green_count=green,
        orange_count=orange,
        purple_count=purple,
        reached_count=reached,
        target_percent=round(target_percent, 1),
        closed_percent=round(closed_percent, 1),
        working_days=working_days
    )
