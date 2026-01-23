# app/utils/dates.py

from datetime import datetime, date

def parse_datetime(value):
    """
    Convierte celdas Excel/strings a datetime cuando sea posible.
    Si no puede, devuelve None.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)

    s = str(value).strip()
    if not s:
        return None

    # formatos comunes (podemos ampliar según tus excels reales)
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue

    return None