# app/utils/strings.py

import re
import unicodedata


def norm_text(value: str) -> str:
    """
    Normaliza texto:
    - string
    - trim
    - colapsa espacios
    - elimina tildes
    - elimina signos raros (¿?°.)
    """
    if value is None:
        return ""

    s = str(value).strip()

    # quitar tildes: Número -> Numero
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")

    # reemplazar símbolos comunes
    s = s.replace("°", "")
    s = s.replace("¿", "")
    s = s.replace("?", "")
    s = s.replace(".", "")

    # colapsar espacios
    s = re.sub(r"\s+", " ", s)

    return s


def upper_clean(value: str) -> str:
    """
    Texto normalizado + UPPER
    """
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