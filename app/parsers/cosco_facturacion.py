# app/parsers/cosco_facturacion.py

from __future__ import annotations

from typing import Dict, List, Any, Optional, Tuple

from openpyxl import load_workbook

from app.parsers.base import BaseParser
from app.parsers.normalization import (
    map_columns_by_synonyms,
    normalize_guia,
    normalize_contenedor,
    normalize_amount,
)


class COSCOFacturacionParser(BaseParser):
    """
    COSCO multihoja. Consolidamos todas las hojas.
    Campos normalizados:
      guia, contenedor (si viene), total_naviera, ruta, predio, sheet

    Cambios para Excel grandes:
    - sniff() liviano sin pandas
    - parse() streaming sin pandas (openpyxl read_only)
    """

    SYNONYMS = {
        "guia": ["Documento", "Guia", "Guía", "No Guia", "No. Documento", "N° Documento"],
        "contenedor": ["Contenedor", "Container", "CNTR"],
        "total": ["Total", "Monto", "Importe", "Total Naviera", "Total Facturado"],
        "ruta": ["Ruta", "Ruta Tipo", "Tipo", "Servicio", "Servicio Facturado"],
        "fecha": ["Fecha", "Fecha Movimiento"],
        "predio": ["Predio", "Patio", "Terminal"],
    }

    def sniff(self, path: str) -> Dict:
        meta: Dict[str, Any] = {"errors": [], "warnings": []}

        try:
            wb = load_workbook(filename=path, read_only=True, data_only=True)
        except Exception as e:
            meta["errors"].append(f"COSCO: no se pudo abrir el archivo (inválido o corrupto): {e}")
            return meta

        try:
            sheets = [ws.title for ws in wb.worksheets]
            meta["sheets"] = sheets

            if not sheets:
                meta["errors"].append("COSCO: el archivo no contiene hojas.")
                return meta

            # Elegir la primera hoja como muestra (puede variar por hoja)
            ws = wb.worksheets[0]
            meta["sheet_used"] = ws.title

            # Leer header fila 1 (si está vacío, intentar fila 2)
            header_row_idx = 1
            header_values = next(ws.iter_rows(min_row=1, max_row=1, values_only=True))
            headers = [("" if c is None else str(c).strip()) for c in header_values]

            if self._is_mostly_empty(headers):
                row2 = next(ws.iter_rows(min_row=2, max_row=2, values_only=True), None)
                if row2:
                    headers2 = [("" if c is None else str(c).strip()) for c in row2]
                    if not self._is_mostly_empty(headers2):
                        headers = headers2
                        header_row_idx = 2
                        meta["warnings"].append("COSCO: encabezados no claros en fila 1; usando fila 2 como encabezado.")

            mapped = map_columns_by_synonyms(headers, self.SYNONYMS)

            if not mapped.get("guia"):
                meta["errors"].append(
                    "COSCO: no se encontró columna Documento/Guía en la hoja de muestra (puede variar por hoja)."
                )
            if not mapped.get("total"):
                meta["errors"].append(
                    "COSCO: no se encontró columna Total/Monto en la hoja de muestra (puede variar por hoja)."
                )

            meta["mapped_sample"] = mapped
            meta["headers_preview"] = headers[:50]
            meta["header_row"] = header_row_idx

        except Exception as e:
            meta["errors"].append(f"COSCO: error leyendo header: {e}")
        finally:
            try:
                wb.close()
            except Exception:
                pass

        return meta

    def parse(self, path: str) -> List[dict]:
        """
        Procesamiento real (pesado) en streaming:
        - Itera por todas las hojas
        - Mapea columnas por sinónimos por hoja (porque puede variar)
        - Lee fila por fila y consolida

        Nota: Esto sigue devolviendo una lista completa, como tu versión original.
        Más adelante (en job_runner) podemos evitar acumular todo y escribir en DB por lotes.
        """
        wb = load_workbook(filename=path, read_only=True, data_only=True)
        rows: List[dict] = []

        try:
            if not wb.worksheets:
                return rows

            for ws in wb.worksheets:
                sheet = ws.title

                # Detectar header (fila 1; si vacío, intentar fila 2)
                header_row_idx = 1
                header_values = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
                if header_values is None:
                    continue

                headers = [("" if c is None else str(c).strip()) for c in header_values]

                if self._is_mostly_empty(headers):
                    row2 = next(ws.iter_rows(min_row=2, max_row=2, values_only=True), None)
                    if row2:
                        headers2 = [("" if c is None else str(c).strip()) for c in row2]
                        if not self._is_mostly_empty(headers2):
                            headers = headers2
                            header_row_idx = 2

                # Si no hay headers útiles, saltar hoja
                if self._is_mostly_empty(headers):
                    continue

                mapped = map_columns_by_synonyms(headers, self.SYNONYMS)
                guia_col = mapped.get("guia")
                total_col = mapped.get("total")

                # Si no trae lo mínimo, ignorar hoja
                if not guia_col or not total_col:
                    continue

                cont_col = mapped.get("contenedor")
                ruta_col = mapped.get("ruta")
                predio_col = mapped.get("predio")

                idx = self._build_index_map(headers)

                def get_cell(row: Tuple[Any, ...], col_name: Optional[str]) -> Any:
                    if not col_name:
                        return None
                    i = idx.get(col_name)
                    if i is None:
                        return None
                    return row[i] if i < len(row) else None

                data_start = header_row_idx + 1

                for row in ws.iter_rows(min_row=data_start, values_only=True):
                    guia = normalize_guia(get_cell(row, guia_col))
                    if not guia:
                        continue

                    total_nav = normalize_amount(get_cell(row, total_col))
                    cont = normalize_contenedor(get_cell(row, cont_col)) if cont_col else ""
                    ruta = str(get_cell(row, ruta_col) or "").strip() if ruta_col else ""
                    predio = str(get_cell(row, predio_col) or "").strip() if predio_col else ""

                    rows.append({
                        "guia": guia,
                        "contenedor": cont,
                        "total_naviera": total_nav or 0,
                        "ruta": ruta,
                        "predio": predio,
                        "sheet": sheet,
                    })

            return rows

        finally:
            try:
                wb.close()
            except Exception:
                pass

    # -----------------------
    # Helpers
    # -----------------------

    def _is_mostly_empty(self, row: List[str]) -> bool:
        if not row:
            return True
        non_empty = sum(1 for c in row if c and str(c).strip())
        return non_empty <= max(1, int(len(row) * 0.05))  # <=5% con contenido

    def _build_index_map(self, headers: List[str]) -> Dict[str, int]:
        """
        Mapa nombre-columna -> índice usando header original (trim).
        Mantiene el nombre exacto tal como lo detectó map_columns_by_synonyms.
        """
        idx: Dict[str, int] = {}
        for i, h in enumerate(headers):
            key = (h or "").strip()
            if key and key not in idx:
                idx[key] = i
        return idx