# app/utils/money.py

import re
from decimal import Decimal, InvalidOperation

def parse_money(value) -> Decimal:
    """
    Convierte valores tipo '₡1,234.50', '1.234,50', '$ 1200', '1200' a Decimal.
    Estrategia:
      - elimina símbolos y espacios
      - detecta separadores comunes
    """
    if value is None:
        return Decimal("0")

    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return Decimal("0")

    s = str(value).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return Decimal("0")

    # quitar moneda y letras
    s = re.sub(r"[^\d,.\-]", "", s)

    # Casos:
    # 1) "1.234,56" -> miles "." decimal ","
    # 2) "1,234.56" -> miles "," decimal "."
    # 3) "1234.56" o "1234,56"
    if s.count(",") > 0 and s.count(".") > 0:
        # el último separador suele ser decimal
        if s.rfind(",") > s.rfind("."):
            # decimal ","
            s = s.replace(".", "")
            s = s.replace(",", ".")
        else:
            # decimal "."
            s = s.replace(",", "")
    else:
        # solo uno de los separadores
        # si hay coma y parece decimal
        if s.count(",") == 1 and s.count(".") == 0:
            s = s.replace(",", ".")
        # si hay puntos múltiples -> miles
        elif s.count(".") > 1:
            s = s.replace(".", "")

    try:
        return Decimal(s)
    except InvalidOperation:
        return Decimal("0")


def money_diff(a, b) -> Decimal:
    return parse_money(a) - parse_money(b)