# app/parsers/fils_auditoria.py

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple, Any

from openpyxl import load_workbook

from app.parsers.base import BaseParser
from app.utils.logging import get_logger
from app.utils.money import parse_money
from app.parsers.normalization import normalize_guia, normalize_contenedor

logger = get_logger("parser_fils_auditoria")


# Nombres de hojas típicos en el reporte FILS
SHEET_GUIA = "Guía"
SHEET_CONTENEDOR = "Contenedor"
SHEET_CARGOS = "Cargos Adicionales"


def _norm_header(s: Any) -> str:
    return str(s or "").strip().lower()


def _parse_fecha(value) -> Optional[datetime]:
    """
    FILS suele traer "01/10/2025 08:49" como string,
    o un datetime nativo.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value

    s = str(value).strip()
    if not s:
        return None

    # formatos comunes
    for fmt in ("%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S", "%Y/%m/%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue

    return None


def _looks_like_header_row(values: List[Any]) -> bool:
    """
    Header válido si contiene palabras clave y varias columnas string.
    """
    cols = [_norm_header(v) for v in values if v is not None and str(v).strip() != ""]
    if len(cols) < 5:
        return False

    joined = " ".join(cols)
    # Palabras clave que sí o sí aparecen en headers FILS
    hits = 0
    for kw in ("numero guia", "número guía", "accion", "acción", "fecha", "estado"):
        if kw in joined:
            hits += 1
    return hits >= 2


def _find_header(ws, max_scan_rows: int = 10) -> Tuple[int, List[str]]:
    """
    Detecta en qué fila está el header.
    Retorna (row_index_1based, headers_lower)
    """
    for r in range(1, max_scan_rows + 1):
        values = [c.value for c in ws[r]]
        if _looks_like_header_row(values):
            headers = [_norm_header(v) for v in values]
            return r, headers
    # fallback: fila 1
    values = [c.value for c in ws[1]]
    headers = [_norm_header(v) for v in values]
    return 1, headers


def _build_index(headers: List[str]) -> Dict[str, int]:
    """
    Mapea columnas relevantes -> índice.
    Se toleran variantes con/ sin tildes y “numero/número”.
    """
    # Normalizamos “número” a “numero” para comparar
    def hnorm(x: str) -> str:
        return (x or "").replace("número", "numero").strip().lower()

    norm_headers = [hnorm(h) for h in headers]

    def find(*cands: str) -> Optional[int]:
        for cand in cands:
            c = hnorm(cand)
            # exact
            for i, h in enumerate(norm_headers):
                if h == c:
                    return i
            # contains
            for i, h in enumerate(norm_headers):
                if c in h:
                    return i
        return None

    idx = {
        # comunes
        "guia": find("numero guia", "número guía", "guia"),
        "accion": find("accion", "acción"),
        "fecha": find("fecha"),
        "estado": find("estado"),
        "ruta": find("ruta"),

        # guía sheet
        "monto_tarifa": find("monto tarifa", "monto total", "total", "monto"),

        # contenedor sheet
        "contenedor": find("contenedor", "container"),
        "contenedor_id": find("contenedor id"),

        # cargos sheet
        "cargo_id": find("cargo id", "id cargo"),
        "cargo": find("cargo", "concepto", "descripcion", "descripción"),
        "monto_naviera": find("monto naviera", "total naviera", "monto"),
    }
    return idx


def _cell(row: List[Any], i: Optional[int]) -> Any:
    if i is None:
        return None
    if i < 0 or i >= len(row):
        return None
    return row[i]


def _cargo_key(cargo_id: Any, cargo_name: Any) -> str:
    cid = str(cargo_id or "").strip()
    if cid:
        return f"ID:{cid}"
    return str(cargo_name or "CARGO").strip().upper()


class FILSAuditoriaParser(BaseParser):
    """
    Parser FILS multi-hoja:
      - Guía: base (estado, fecha, ruta, monto tarifa)
      - Contenedor: guía -> contenedor (para ONE sin guía)
      - Cargos Adicionales: guía -> cargos (última acción; si última es Eliminar, se omite)
    """

    def sniff(self, path: str) -> Dict:
        issues = []
        meta = {}

        wb = load_workbook(path, read_only=True, data_only=True)

        sheets = wb.sheetnames
        meta["sheets"] = sheets

        def has(name: str) -> bool:
            return name in sheets

        if not has(SHEET_GUIA):
            issues.append({"level": "ERROR", "message": f"Falta hoja '{SHEET_GUIA}' en FILS."})
        if not has(SHEET_CONTENEDOR):
            issues.append({"level": "WARN", "message": f"Falta hoja '{SHEET_CONTENEDOR}'. ONE sin guía podría no matchear."})
        if not has(SHEET_CARGOS):
            issues.append({"level": "WARN", "message": f"Falta hoja '{SHEET_CARGOS}'. No se podrán comparar cargos adicionales."})

        # Header preview de Guía
        if has(SHEET_GUIA):
            ws = wb[SHEET_GUIA]
            hr, headers = _find_header(ws)
            meta["guia_header_row"] = hr
            meta["guia_headers_preview"] = headers[:30]

            idx = _build_index(headers)
            missing = [k for k in ("guia", "fecha", "estado") if idx.get(k) is None]
            if missing:
                issues.append({"level": "ERROR", "message": f"Hoja '{SHEET_GUIA}': faltan columnas requeridas: {missing}."})

        wb.close()
        ok = not any(i["level"] == "ERROR" for i in issues)
        return {"ok": ok, "issues": issues, "meta": meta}

    # ✅ ESTE ES EL MÉTODO QUE TE ESTÁ FALTANDO EN PRODUCCIÓN
    def iter_rows(self, path: str, sheet: str = SHEET_GUIA, header_row: Optional[int] = None) -> Iterable[Tuple[List[str], List[Any]]]:
        """
        Itera filas de una hoja específica, devolviendo:
          (headers_lower, row_values_list)

        Si header_row no cuadra con un header real, detecta automáticamente.
        """
        wb = load_workbook(path, read_only=True, data_only=True)

        if sheet not in wb.sheetnames:
            wb.close()
            raise ValueError(f"FILS: no existe la hoja '{sheet}'. Hojas: {wb.sheetnames}")

        ws = wb[sheet]

        # decidir header row
        if header_row is not None:
            # validamos que realmente parezca header
            values = [c.value for c in ws[header_row]]
            if not _looks_like_header_row(values):
                # buscar automático
                hr, headers = _find_header(ws)
                logger.warning(f"Encabezado no detectado claramente en la fila {header_row}; usando fila {hr} como encabezado en '{sheet}'.")
            else:
                hr = header_row
                headers = [_norm_header(v) for v in values]
        else:
            hr, headers = _find_header(ws)

        # recorrer filas posteriores al header
        for r in ws.iter_rows(min_row=hr + 1, values_only=True):
            row = list(r)
            if all(v is None or str(v).strip() == "" for v in row):
                continue
            yield headers, row

        wb.close()

    def parse(self, path: str) -> List[dict]:
        """
        Retorna filas por guía (eventos de la hoja Guía) con:
          - contenedor (desde hoja Contenedor)
          - cargos (desde hoja Cargos Adicionales, última acción por cargo, excluye Eliminar)
        """
        wb = load_workbook(path, read_only=True, data_only=True)
        try:
            if SHEET_GUIA not in wb.sheetnames:
                raise ValueError(f"FILS: falta hoja '{SHEET_GUIA}'.")

            # 1) Mapa guía -> contenedor (último por fecha)
            guia_to_cont: Dict[str, Tuple[Optional[datetime], str]] = {}
            if SHEET_CONTENEDOR in wb.sheetnames:
                ws = wb[SHEET_CONTENEDOR]
                hr, headers = _find_header(ws)
                idx = _build_index(headers)

                if idx.get("guia") is None:
                    raise ValueError("FILS/Contenedor: no se encontró columna 'Número Guía'.")
                if idx.get("contenedor") is None:
                    raise ValueError("FILS/Contenedor: no se encontró columna 'Contenedor'.")

                for row in ws.iter_rows(min_row=hr + 1, values_only=True):
                    row = list(row)
                    g = normalize_guia(_cell(row, idx["guia"]))
                    if not g:
                        continue

                    cont = normalize_contenedor(_cell(row, idx["contenedor"]))
                    cont = cont.replace("-", "")  # normalización extra
                    if not cont:
                        continue

                    f = _parse_fecha(_cell(row, idx.get("fecha")))
                    prev = guia_to_cont.get(g)
                    # quedarnos con el más reciente
                    if prev is None or ((f or datetime.min) >= (prev[0] or datetime.min)):
                        guia_to_cont[g] = (f, cont)

            # 2) Mapa guía -> cargos (última acción por cargo key)
            #    key = cargo_id si existe, si no cargo nombre.
            guia_to_cargos: Dict[str, Dict[str, dict]] = {}
            if SHEET_CARGOS in wb.sheetnames:
                ws = wb[SHEET_CARGOS]
                hr, headers = _find_header(ws)
                idx = _build_index(headers)

                if idx.get("guia") is None:
                    raise ValueError("FILS/Cargos Adicionales: no se encontró columna 'Número Guía'.")
                if idx.get("accion") is None:
                    raise ValueError("FILS/Cargos Adicionales: no se encontró columna 'Acción'.")
                if idx.get("fecha") is None:
                    raise ValueError("FILS/Cargos Adicionales: no se encontró columna 'Fecha'.")
                if idx.get("monto_naviera") is None:
                    raise ValueError("FILS/Cargos Adicionales: no se encontró columna 'Monto Naviera' (o Total Naviera).")
                if idx.get("cargo") is None and idx.get("cargo_id") is None:
                    raise ValueError("FILS/Cargos Adicionales: no se encontró columna de 'Cargo' ni 'Cargo Id'.")

                # guardamos el último evento por (guía, cargo_key)
                for row in ws.iter_rows(min_row=hr + 1, values_only=True):
                    row = list(row)

                    g = normalize_guia(_cell(row, idx["guia"]))
                    if not g:
                        continue

                    accion = str(_cell(row, idx["accion"]) or "").strip().lower()
                    f = _parse_fecha(_cell(row, idx["fecha"]))
                    cargo_id = _cell(row, idx.get("cargo_id"))
                    cargo_name = _cell(row, idx.get("cargo"))
                    key = _cargo_key(cargo_id, cargo_name)

                    monto = parse_money(_cell(row, idx["monto_naviera"]))

                    event = {
                        "cargo_id": str(cargo_id or "").strip(),
                        "cargo": str(cargo_name or "").strip() if cargo_name is not None else "",
                        "tipo_cargo": key,  # para reconciliation
                        "accion": accion,
                        "fecha": f,
                        "monto": monto,
                    }

                    guia_to_cargos.setdefault(g, {})
                    prev = guia_to_cargos[g].get(key)
                    if prev is None:
                        guia_to_cargos[g][key] = event
                    else:
                        # escoger evento más reciente (si fecha None, conservamos el existente)
                        if (f or datetime.min) >= (prev.get("fecha") or datetime.min):
                            guia_to_cargos[g][key] = event

                # filtrar eliminados (última acción = eliminar)
                for g in list(guia_to_cargos.keys()):
                    filtered = {}
                    for key, ev in guia_to_cargos[g].items():
                        if str(ev.get("accion") or "").strip().lower() == "eliminar":
                            continue
                        filtered[key] = ev
                    guia_to_cargos[g] = filtered

            # 3) Leer hoja Guía (eventos) y anexar contenedor + cargos
            ws = wb[SHEET_GUIA]
            hr, headers = _find_header(ws)
            idx = _build_index(headers)

            # requeridos para el flujo
            if idx.get("guia") is None:
                raise ValueError("FILS/Guía: no se encontró columna 'Número Guía'.")
            if idx.get("fecha") is None:
                raise ValueError("FILS/Guía: no se encontró columna 'Fecha'.")
            if idx.get("estado") is None:
                raise ValueError("FILS/Guía: no se encontró columna 'Estado'.")

            out: List[dict] = []

            for row in ws.iter_rows(min_row=hr + 1, values_only=True):
                row = list(row)

                g = normalize_guia(_cell(row, idx["guia"]))
                if not g:
                    continue

                estado = str(_cell(row, idx["estado"]) or "").strip().upper()
                fecha = _parse_fecha(_cell(row, idx["fecha"]))
                ruta = str(_cell(row, idx.get("ruta")) or "").strip()

                monto_tarifa = parse_money(_cell(row, idx.get("monto_tarifa")))

                # contenedor: si no está en Guía, lo tomamos de Contenedor
                cont = ""
                if g in guia_to_cont:
                    cont = guia_to_cont[g][1]

                # cargos: lista de dicts
                cargos_map = guia_to_cargos.get(g, {})
                cargos_list = []
                for _, ev in cargos_map.items():
                    cargos_list.append(
                        {
                            "cargo_id": ev.get("cargo_id") or "",
                            "cargo": ev.get("cargo") or "",
                            "tipo_cargo": ev.get("tipo_cargo") or "CARGO",
                            "monto": ev.get("monto") or parse_money(0),
                            "fecha": ev.get("fecha"),
                        }
                    )

                out.append(
                    {
                        "guia": g,
                        "contenedor": cont,  # normalizado
                        "estado": estado,
                        "fecha": fecha,
                        "fecha_cierre": fecha,  # reconcile usa esto para ordenar
                        "ruta": ruta,

                        # Base para comparar + cargos naviera se suman en reconciliation
                        "monto_total": monto_tarifa,

                        # compat
                        "monto_flete": None,
                        "monto_extras": None,

                        # clave
                        "cargos": cargos_list,
                    }
                )

            return out

        finally:
            wb.close()

