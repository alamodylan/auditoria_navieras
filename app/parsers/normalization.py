# app/parsers/normalization.py

from typing import Dict, List, Optional
import re

from app.utils.money import parse_money
from app.utils.strings import upper_clean

def normalize_guia(value) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    s = re.sub(r"\s+", "", s)
    return s

def normalize_contenedor(value) -> str:
    if value is None:
        return ""
    s = str(value).strip().upper()
    s = re.sub(r"\s+", "", s)
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
    """
    cols_up = {c: upper_clean(c) for c in columns}
    rev = {upper_clean(c): c for c in columns}

    mapped = {}
    for canon, opts in synonyms.items():
        found = None
        for o in opts:
            o_up = upper_clean(o)
            # match exact
            if o_up in rev:
                found = rev[o_up]
                break
            # match contains (por si viene “Documento No.”)
            for cu, orig in rev.items():
                if o_up in cu:
                    found = orig
                    break
            if found:
                break
        mapped[canon] = found
    return mapped