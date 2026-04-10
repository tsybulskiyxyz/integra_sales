"""Расчёт статистики по звонкам."""
from models import CallRow, Stats, RowStatus


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
