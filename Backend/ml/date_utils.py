# ml/date_utils.py
from datetime import date

def first_day_of_month(d: date) -> date:
    return date(d.year, d.month, 1)

def prev_month_start(d: date) -> date:
    """
    Devuelve el primer día del mes anterior a d.
    Ej: 2025-03-15 -> 2025-02-01
    """
    y, m = d.year, d.month - 1
    if m == 0:
        m = 12
        y -= 1
    return date(y, m, 1)
