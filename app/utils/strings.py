# app/utils/strings.py

import re

def norm_text(value: str) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def upper_clean(value: str) -> str:
    return norm_text(value).upper()

def find_route_tokens(*fields) -> set:
    """
    Busca tokens SJO / CAL / LIO en varios campos de texto.
    """
    tokens = set()
    joined = " ".join([upper_clean(f) for f in fields if f is not None])
    for t in ("SJO", "CAL", "LIO"):
        if t in joined:
            tokens.add(t)
    return tokens