# app/parsers/one_facturacion.py

from __future__ import annotations

from typing import Dict, List, Any, Optional, Iterator, Tuple

from openpyxl import load_workbook

from app.parsers.base import BaseParser
from app.parsers.normalization import (
    map_columns_by_synonyms,
    normalize_guia,
    normalize_contenedor,
    normalize_amount,
)


class ONEFacturacionParser(BaseParser):
    """
    ONE puede variar mucho. MVP:
    - sniff() liviano: detecta hojas y mapea columnas con sinónimos leyendo solo header + pocas filas.
    - parse() streaming: procesa fila por fila con openpyxl read_only para Excel grandes.
    """

    SYNONYMS = {
        "guia": ["Guia", "Guía", "Documento", "No Documento", "Referencia", "Reference"],
        "contenedor": ["Contenedor", "Container", "CNTR"],
        "total": ["Total", "Monto", "Importe", "Amount", "Total Facturado"],
        "ruta": ["Ruta", "Servicio", "Service", "Tipo", "Servicio Facturado"],
    }

    def sniff(self, path: str) -> Dict:
        meta: Dict[str, Any] = {"errors": [], "warnings": []}

        try:
            wb = load_workbook(filename=path, read_only=True, data_only=True)
        except Exception as e:
            meta["errors"].append(f"ONE: no se pudo abrir el archivo (inválido o corrupto): {e}")
            return meta

        try:
            sheets = [ws.title for ws in wb.worksheets]
            meta["sheets"] = sheets

            if not wb.worksheets:
                meta["errors"].append("ONE: el archivo no contiene hojas.")
                return meta

            ws = wb.worksheets[0]
            meta["sheet_used"] = ws.title

            # Leer SOLO el header (fila 1) + 2 filas de muestra (opcional)
            header_values = next(ws.iter_rows(min_row=1, max_row=1, values_only=True))
            headers = [("" if c is None else str(c).strip()) for c in header_values]

            # Si el header viene vacío o mal, probamos fila 2 como header
            if self._is_mostly_empty(headers):
                row2 = next(ws.iter_rows(min_row=2, max_row=2, values_only=True), None)
                if row2:
                    headers2 = [("" if c is None else str(c).strip()) for c in row2]
                    if not self._is_mostly_empty(headers2):
                        meta["warnings"].append("ONE: encabezados no claros en fila 1; usando fila 2 como encabezado.")
                        headers = headers2

            mapped = map_columns_by_synonyms(headers, self.SYNONYMS)

            if not mapped.get("guia"):
                meta["errors"].append("ONE: no se encontró columna Guía/Documento/Referencia.")
            if not mapped.get("total"):
                meta["errors"].append("ONE: no se encontró columna Total/Monto.")

            meta["mapped_sample"] = mapped
            meta["headers_preview"] = headers[:50]

        except Exception as e:
            meta["errors"].append(f"ONE: error leyendo header: {e}")
        finally:
            try:
                wb.close()
            except Exception:
                pass

        return meta

    def parse(self, path: str) -> List[dict]:
        """
        Procesamiento real (pesado) en streaming:
        - Abre la primera hoja
        - Mapea columnas con sinónimos usando el header
        - Itera filas de datos sin cargar todo a memoria
        """
        wb = load_workbook(filename=path, read_only=True, data_only=True)
        try:
            if not wb.worksheets:
                raise ValueError("ONE: el archivo no contiene hojas.")

            ws = wb.worksheets[0]
            sheet_name = ws.title

            # Detectar header (fila 1; si es vacío, intentar fila 2)
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

            mapped = map_columns_by_synonyms(headers, self.SYNONYMS)

            guia_col = mapped.get("guia")
            total_col = mapped.get("total")
            if not guia_col or not total_col:
                raise ValueError("ONE: columnas mínimas no encontradas (guía/total).")

            cont_col = mapped.get("contenedor")
            ruta_col = mapped.get("ruta")

            # Índices por nombre de columna detectado
            idx = self._build_index_map(headers)

            def get_cell(row: Tuple[Any, ...], col_name: Optional[str]) -> Any:
                if not col_name:
                    return None
                i = idx.get(col_name)
                if i is None:
                    return None
                return row[i] if i < len(row) else None

            rows: List[dict] = []

            data_start = header_row_idx + 1
            for row in ws.iter_rows(min_row=data_start, values_only=True):
                guia = normalize_guia(get_cell(row, guia_col))
                if not guia:
                    continue

                rows.append({
                    "guia": guia,
                    "contenedor": normalize_contenedor(get_cell(row, cont_col)) if cont_col else "",
                    "total_naviera": normalize_amount(get_cell(row, total_col)) or 0,
                    "ruta": str(get_cell(row, ruta_col) or "").strip() if ruta_col else "",
                    "sheet": sheet_name,
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
        Crea un mapa de nombre-columna -> índice usando el header original (trim).
        Mantiene el nombre exacto tal como lo detectó map_columns_by_synonyms.
        """
        idx: Dict[str, int] = {}
        for i, h in enumerate(headers):
            key = (h or "").strip()
            if key and key not in idx:
                idx[key] = i
        return idx