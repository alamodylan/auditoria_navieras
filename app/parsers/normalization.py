# app/parsers/normalization.py

from typing import Dict, List, Optional
import re

from app.utils.money import parse_money
from app.utils.strings import upper_clean


def normalize_guia(value) -> str:
    """
    Normaliza guía:
    - string
    - trim
    - quita espacios
    - quita guiones (por si algún reporte viene 0000-1234)
    """
    if value is None:
        return ""
    s = str(value).strip()
    s = re.sub(r"\s+", "", s)
    s = s.replace("-", "")
    return s


def normalize_contenedor(value) -> str:
    """
    Normaliza contenedor:
    - upper
    - quita espacios
    - quita guiones
    Ej: CSNU-123456-7 -> CSNU1234567
    """
    if value is None:
        return ""
    s = str(value).strip().upper()
    s = re.sub(r"\s+", "", s)
    s = s.replace("-", "")
    return s


def normalize_amount(value):
    return parse_money(value)


def pick_first_existing(row: Dict, keys: List[str], default=None):
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    return default


def map_columns_by_synonyms(columns: List[str], synonyms: Dict[str, List[str]]) -> Dict[str, Optional[str]]:
    """
    columns: lista columnas del excel (tal como vienen)
    synonyms: {canonical: [opcion1, opcion2, ...]}
    Retorna: {canonical: columna_real_encontrada_o_None}

    Matching:
    - exact por upper_clean
    - contains por upper_clean (para casos tipo "Documento No.")
    """
    rev = {upper_clean(c): c for c in columns}

    mapped: Dict[str, Optional[str]] = {}
    for canon, opts in synonyms.items():
        found = None

        for o in opts:
            o_up = upper_clean(o)

            # exact
            if o_up in rev:
                found = rev[o_up]
                break

            # contains
            for cu, orig in rev.items():
                if o_up and o_up in cu:
                    found = orig
                    break

            if found:
                break

        mapped[canon] = found

    return mapped