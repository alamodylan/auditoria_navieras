# app/utils/money.py

import re
from decimal import Decimal, InvalidOperation


def parse_money(value) -> Decimal:
    """
    Convierte valores tipo '₡1,234.50', '1.234,50', '$ 1200', '1200' a Decimal.
    Refuerzos:
      - soporta negativos con paréntesis: (1,234.50) -> -1234.50
      - soporta negativos al final: 1,234.50- -> -1234.50
      - maneja floats/ints/Decimal y strings sucias
    """
    if value is None:
        return Decimal("0")

    # Números directos
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        try:
            # str(float) suele venir bien (ej "124797.0"), pero puede venir "1e+06"
            return Decimal(str(value))
        except InvalidOperation:
            return Decimal("0")

    s = str(value).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return Decimal("0")

    # Detectar negativos con paréntesis
    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1].strip()

    # Negativo al final (ej: "123.45-")
    if s.endswith("-"):
        negative = True
        s = s[:-1].strip()

    # Quitar moneda/letras, dejar dígitos, separadores y signo -
    s = re.sub(r"[^\d,.\-]", "", s)

    # Si queda solo "-" o vacío
    if s in ("", "-"):
        return Decimal("0")

    # Normalización de separadores:
    # 1) "1.234,56" -> miles "." decimal ","
    # 2) "1,234.56" -> miles "," decimal "."
    # 3) "1234.56" o "1234,56"
    if s.count(",") > 0 and s.count(".") > 0:
        # El último separador suele ser el decimal
        if s.rfind(",") > s.rfind("."):
            # decimal ","
            s = s.replace(".", "")
            s = s.replace(",", ".")
        else:
            # decimal "."
            s = s.replace(",", "")
    else:
        # Solo uno de los separadores
        if s.count(",") == 1 and s.count(".") == 0:
            # "1234,56" -> "1234.56"
            s = s.replace(",", ".")
        elif s.count(".") > 1:
            # "1.234.567" -> "1234567"
            s = s.replace(".", "")

    try:
        val = Decimal(s)
        return -val if negative else val
    except InvalidOperation:
        return Decimal("0")


def money_diff(a, b) -> Decimal:
    return parse_money(a) - parse_money(b)