# app/parsers/fils_auditoria.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Iterator, Tuple

from openpyxl import load_workbook


@dataclass
class SniffResult:
    ok: bool
    sheet: str
    headers: List[str]
    errors: List[str]
    warnings: List[str]
    sample_rows: List[List[str]]


class FILSAuditoriaParser:
    """
    Parser para el reporte de auditoría FILS.

    IMPORTANTÍSIMO:
    - sniff() debe ser LIVIANO: solo lee pocas filas (header + sample).
    - iter_rows() es para lectura pesada (streaming) usada en job_runner.
    """

    # Ajusta estos a los encabezados reales de tu reporte FILS
    # (ponemos heurística "contiene" para soportar variaciones).
    REQUIRED_HEADER_HINTS = [
        "guia",          # guía / guía viaje / numero guia
        "contenedor",    # contenedor
        "monto",         # monto / total / monto total
    ]

    def sniff(self, path: str) -> Dict[str, Any]:
        """
        Precheck ligero: abre el XLSX en read_only, lee 1-3 filas y valida encabezados.
        NO usa pandas.
        """
        errors: List[str] = []
        warnings: List[str] = []

        try:
            wb = load_workbook(filename=path, read_only=True, data_only=True)
        except Exception as e:
            return {
                "ok": False,
                "sheet": "",
                "headers": [],
                "errors": [f"No se pudo abrir el Excel FILS (archivo inválido o corrupto): {e}"],
                "warnings": [],
                "sample_rows": [],
            }

        try:
            ws = wb.worksheets[0]
            sheet_name = ws.title

            # Lee solo pocas filas (1 header + 2 samples)
            raw_rows: List[List[str]] = []
            for row in ws.iter_rows(min_row=1, max_row=3, values_only=True):
                raw_rows.append([self._cell_to_str(c) for c in row])

            if not raw_rows:
                errors.append("El archivo FILS no contiene filas visibles en la primera hoja.")
                return {
                    "ok": False,
                    "sheet": sheet_name,
                    "headers": [],
                    "errors": errors,
                    "warnings": warnings,
                    "sample_rows": [],
                }

            # Intento 1: header en fila 1
            header = self._normalize_headers(raw_rows[0])

            # Si fila 1 está vacía o no parece encabezado, intentar fila 2
            if self._is_mostly_empty(header) and len(raw_rows) >= 2:
                header2 = self._normalize_headers(raw_rows[1])
                if not self._is_mostly_empty(header2):
                    warnings.append("Encabezado no detectado claramente en la fila 1; usando fila 2 como encabezado.")
                    header = header2
                else:
                    errors.append("No se detectaron encabezados válidos en las primeras filas (1-2).")

            if not header or self._is_mostly_empty(header):
                errors.append("Encabezado vacío o ilegible. Revise el formato del Excel FILS.")
                return {
                    "ok": False,
                    "sheet": sheet_name,
                    "headers": header or [],
                    "errors": errors,
                    "warnings": warnings,
                    "sample_rows": raw_rows[1:] if len(raw_rows) > 1 else [],
                }

            # Validación flexible: que existan hints requeridos en el header
            missing = self._missing_required(header)
            if missing:
                errors.append(f"Faltan columnas requeridas (por heurística): {missing}. "
                              f"Encabezados detectados: {header[:30]}")

            # Señales de archivo “pesado” (no falla, solo advierte)
            # Nota: en read_only openpyxl no siempre calcula dimensiones de forma barata,
            # así que no intentamos ws.max_row ni used_range aquí.
            if len(header) > 150:
                warnings.append("Se detectaron muchos encabezados/columnas. Verifique que sea el reporte correcto.")

            ok = len(errors) == 0

            # sample_rows: devolvemos 2 filas de muestra (sin header)
            sample_rows = raw_rows[1:] if len(raw_rows) > 1 else []

            return {
                "ok": ok,
                "sheet": sheet_name,
                "headers": header,
                "errors": errors,
                "warnings": warnings,
                "sample_rows": sample_rows,
            }

        finally:
            try:
                wb.close()
            except Exception:
                pass

    def iter_rows(
        self,
        path: str,
        sheet_index: int = 0,
        header_row: int = 1,
        start_data_row: Optional[int] = None,
        max_rows: Optional[int] = None,
    ) -> Iterator[Tuple[List[str], Tuple[Any, ...]]]:
        """
        Lectura streaming (para job_runner):
        - Abre en read_only
        - Lee encabezado en header_row
        - Itera filas de datos desde start_data_row (si None, header_row+1)
        - No carga DataFrame completo.

        Yields: (headers_normalizados, row_values_originales)
        """
        wb = load_workbook(filename=path, read_only=True, data_only=True)
        try:
            ws = wb.worksheets[sheet_index]

            # Leer header
            header_cells = next(
                ws.iter_rows(min_row=header_row, max_row=header_row, values_only=True)
            )
            headers = self._normalize_headers([self._cell_to_str(c) for c in header_cells])

            data_row = start_data_row if start_data_row is not None else header_row + 1

            count = 0
            for row in ws.iter_rows(min_row=data_row, values_only=True):
                yield headers, row
                count += 1
                if max_rows is not None and count >= max_rows:
                    break
        finally:
            try:
                wb.close()
            except Exception:
                pass

    # -----------------------
    # Helpers
    # -----------------------

    def _cell_to_str(self, value: Any) -> str:
        if value is None:
            return ""
        try:
            s = str(value).strip()
        except Exception:
            return ""
        return s

    def _normalize_headers(self, headers: List[str]) -> List[str]:
        """
        Normaliza encabezados:
        - lower
        - trim
        - colapsa espacios
        """
        out: List[str] = []
        for h in headers:
            h2 = " ".join((h or "").strip().lower().split())
            out.append(h2)
        return out

    def _is_mostly_empty(self, row: List[str]) -> bool:
        if not row:
            return True
        non_empty = sum(1 for c in row if c and c.strip())
        return non_empty <= max(1, int(len(row) * 0.05))  # <=5% con contenido

    def _missing_required(self, headers: List[str]) -> List[str]:
        """
        Requeridos por "hints": si cualquier header contiene el hint, se considera presente.
        """
        missing: List[str] = []
        for hint in self.REQUIRED_HEADER_HINTS:
            if not any(hint in h for h in headers):
                missing.append(hint)
        return missing