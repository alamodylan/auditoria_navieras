# app/parsers/fils_auditoria.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Iterator, Tuple
from datetime import datetime
import unicodedata

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

    Objetivo real (según tu caso):
    - Hoja "Guía": trae el total a comparar (p.ej. "Monto Tarifa") + estado + fecha + ruta, etc.
      OJO: aquí puede NO venir el contenedor.
    - Hoja "Contenedor": trae Número Guía + Contenedor (para poder ligar por contenedor cuando ONE no trae guía).
    - Hoja "Cargos Adicionales": trae Número Guía + Cargo + Monto Naviera y hay que tomar el ÚLTIMO evento
      y omitir si la última acción es "Eliminado".

    IMPORTANTÍSIMO:
    - sniff() debe ser liviano (solo filas iniciales).
    - iter_rows() es streaming (read_only) para jobs grandes.
    """

    # Nombres de hojas típicas (no exact match: usamos "contains")
    SHEET_HINTS = {
        "guia": ["guia", "guía"],
        "contenedor": ["contenedor", "container"],
        "cargos": ["cargos", "cargo adicional", "cargos adicionales", "adicionales"],
    }

    # Hints de columnas (sin acentos, lower, espacios colapsados)
    COL_HINTS = {
        "guia": ["numero guia", "número guía", "número guia", "numero guía"],
        "contenedor": ["contenedor", "container", "cntr"],
        "fecha": ["fecha", "fecha llegada"],
        "estado": ["estado"],
        "ruta": ["ruta"],
        "monto_tarifa": ["monto tarifa"],        # <- total para comparar
        "monto_flete": ["monto flete", "flete"], # opcional
        # Cargos Adicionales
        "cargo": ["cargo"],
        "monto_naviera": ["monto naviera", "total naviera", "monto"],
        "accion": ["accion", "acción"],
    }

    # Requeridos POR HOJA (para no tirar errores falsos)
    REQUIRED_GUIDE_SHEET = ["guia", "monto_tarifa"]          # contenedor NO es requerido aquí
    REQUIRED_CONTAINER_SHEET = ["guia", "contenedor"]
    REQUIRED_CARGOS_SHEET = ["guia", "cargo", "monto_naviera", "accion", "fecha"]

    # -----------------------
    # API pública
    # -----------------------

    def sniff(self, path: str) -> Dict[str, Any]:
        """
        Precheck ligero:
        - abre el XLSX en read_only
        - detecta hojas (guia/contenedor/cargos) por nombre
        - detecta fila de header real (a veces el header está en fila 2)
        - valida columnas mínimas por hoja (sin obligar contenedor en "Guía")
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
                "sheets": [],
            }

        try:
            sheet_names = [ws.title for ws in wb.worksheets]
            meta: Dict[str, Any] = {
                "ok": False,
                "sheet": "",
                "headers": [],
                "errors": errors,
                "warnings": warnings,
                "sample_rows": [],
                "sheets": sheet_names,
                "detected": {},  # info por hoja detectada
            }

            if not sheet_names:
                errors.append("El archivo FILS no contiene hojas.")
                return meta

            # Detectar hojas por "contains"
            guia_sheet = self._find_sheet_name(sheet_names, self.SHEET_HINTS["guia"])
            cont_sheet = self._find_sheet_name(sheet_names, self.SHEET_HINTS["contenedor"])
            cargos_sheet = self._find_sheet_name(sheet_names, self.SHEET_HINTS["cargos"])

            if not guia_sheet:
                errors.append("FILS: no se detectó hoja de 'Guía' (por nombre).")
            if not cont_sheet:
                warnings.append("FILS: no se detectó hoja 'Contenedor'. (Si ONE no trae guía, esta hoja es necesaria).")
            if not cargos_sheet:
                warnings.append("FILS: no se detectó hoja 'Cargos Adicionales'. (Si vas a auditar cargos, es necesaria).")

            # Validar hoja guía (si existe)
            if guia_sheet:
                det = self._sniff_one_sheet(wb, guia_sheet, required=self.REQUIRED_GUIDE_SHEET)
                meta["detected"]["guia"] = det
                meta["sheet"] = guia_sheet
                meta["headers"] = det.get("headers", [])
                meta["sample_rows"] = det.get("sample_rows", [])
                # “header no en fila 1”
                if det.get("header_row", 1) != 1:
                    warnings.append(
                        f"Encabezado no detectado claramente en la fila 1; usando fila {det.get('header_row')} como encabezado."
                    )
                # anexar errores/warnings de esa hoja
                errors.extend(det.get("errors", []))
                warnings.extend(det.get("warnings", []))

            # Validar contenedor (si existe)
            if cont_sheet:
                det = self._sniff_one_sheet(wb, cont_sheet, required=self.REQUIRED_CONTAINER_SHEET)
                meta["detected"]["contenedor"] = det
                # Si falta algo aquí, normalmente sí debe ser ERROR (porque es clave para ONE sin guía)
                # pero si todavía no estás usando ONE-sin-guia, podés bajarlo a WARN.
                errors.extend(det.get("errors", []))
                warnings.extend(det.get("warnings", []))

            # Validar cargos (si existe)
            if cargos_sheet:
                det = self._sniff_one_sheet(wb, cargos_sheet, required=self.REQUIRED_CARGOS_SHEET)
                meta["detected"]["cargos_adicionales"] = det
                # Cargos no siempre aplican, por eso lo dejamos en WARN si falla
                # (para no bloquear jobs donde no se auditen cargos aún).
                if det.get("errors"):
                    warnings.extend([f"(Cargos Adicionales) {m}" for m in det["errors"]])
                warnings.extend(det.get("warnings", []))

            meta["ok"] = len(errors) == 0
            return meta

        finally:
            try:
                wb.close()
            except Exception:
                pass

    def iter_rows(
        self,
        path: str,
        sheet_index: int = 0,
        sheet_name: Optional[str] = None,
        header_row: int = 1,
        start_data_row: Optional[int] = None,
        max_rows: Optional[int] = None,
    ) -> Iterator[Tuple[List[str], Tuple[Any, ...]]]:
        """
        Lectura streaming:
        - Si sheet_name viene, usa esa hoja; si no, usa sheet_index.
        - Lee encabezado en header_row.
        - Itera datos desde start_data_row (si None, header_row+1).

        Yields: (headers_normalizados, row_values_originales)
        """
        wb = load_workbook(filename=path, read_only=True, data_only=True)
        try:
            if sheet_name:
                if sheet_name not in wb.sheetnames:
                    raise ValueError(f"FILS: hoja '{sheet_name}' no existe en el archivo.")
                ws = wb[sheet_name]
            else:
                ws = wb.worksheets[sheet_index]

            header_cells = next(ws.iter_rows(min_row=header_row, max_row=header_row, values_only=True))
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
    # Internals: sniff sheet
    # -----------------------

    def _sniff_one_sheet(self, wb, sheet_name: str, required: List[str]) -> Dict[str, Any]:
        """
        Lee pocas filas de una hoja y:
        - encuentra fila de header (1..10)
        - normaliza headers
        - valida required (por hints)
        """
        errors: List[str] = []
        warnings: List[str] = []

        ws = wb[sheet_name]

        # leer primeras filas para detectar header real
        raw_rows: List[List[str]] = []
        max_probe = 10
        for row in ws.iter_rows(min_row=1, max_row=max_probe, values_only=True):
            raw_rows.append([self._cell_to_str(c) for c in row])

        if not raw_rows:
            return {
                "sheet": sheet_name,
                "ok": False,
                "header_row": 1,
                "headers": [],
                "errors": [f"Hoja '{sheet_name}' no contiene filas visibles."],
                "warnings": [],
                "sample_rows": [],
            }

        # detectar header row buscando una fila con suficientes celdas llenas
        # y que contenga al menos "numero guia" o similares
        header_row = self._detect_header_row(raw_rows)

        if header_row is None:
            errors.append(f"No se detectaron encabezados válidos en la hoja '{sheet_name}' (filas 1-{max_probe}).")
            return {
                "sheet": sheet_name,
                "ok": False,
                "header_row": 1,
                "headers": [],
                "errors": errors,
                "warnings": warnings,
                "sample_rows": raw_rows[:3],
            }

        header = self._normalize_headers(raw_rows[header_row - 1])

        # Validación required (por hints)
        missing = self._missing_required_by_keys(header, required)
        if missing:
            # En texto humano para debug
            errors.append(
                f"Faltan columnas requeridas (por sinónimos) en hoja '{sheet_name}': {missing}. "
                f"Encabezados detectados: {header[:60]}"
            )

        # sample_rows: 2 filas después del header (si existen)
        sample_rows = []
        start = header_row
        for i in range(start, min(start + 2, len(raw_rows))):
            sample_rows.append(raw_rows[i])

        return {
            "sheet": sheet_name,
            "ok": len(errors) == 0,
            "header_row": header_row,
            "headers": header,
            "errors": errors,
            "warnings": warnings,
            "sample_rows": sample_rows,
        }

    def _detect_header_row(self, raw_rows: List[List[str]]) -> Optional[int]:
        """
        Heurística:
        - fila con >= 30% de celdas no vacías
        - y que contenga algo tipo "numero guia" / "guia"
        Retorna número de fila (1-indexed) o None.
        """
        for idx, row in enumerate(raw_rows, start=1):
            norm = self._normalize_headers(row)
            if self._is_mostly_empty(norm):
                continue
            # ¿parece header? buscamos tokens típicos
            joined = " | ".join(norm)
            if ("numero guia" in joined) or ("número guía" in joined) or ("guia" in joined):
                return idx

        # fallback: primera fila no vacía
        for idx, row in enumerate(raw_rows, start=1):
            norm = self._normalize_headers(row)
            if not self._is_mostly_empty(norm):
                return idx
        return None

    def _missing_required_by_keys(self, headers: List[str], required_keys: List[str]) -> List[str]:
        """
        required_keys: ["guia", "monto_tarifa", ...]
        Usa COL_HINTS para ver si existe alguna columna que contenga los hints.
        """
        missing: List[str] = []
        for key in required_keys:
            hints = self.COL_HINTS.get(key, [])
            if not hints:
                continue
            if not self._has_any_hint(headers, hints):
                missing.append(key)
        return missing

    # -----------------------
    # Helpers
    # -----------------------

    def _find_sheet_name(self, sheet_names: List[str], hints: List[str]) -> Optional[str]:
        """
        Busca por "contains" sin acentos / case-insensitive.
        """
        norm_sheets = [(s, self._norm(s)) for s in sheet_names]
        for hint in hints:
            h = self._norm(hint)
            for original, ns in norm_sheets:
                if h in ns:
                    return original
        return None

    def _has_any_hint(self, headers: List[str], hints: List[str]) -> bool:
        for h in headers:
            hh = self._norm(h)
            for hint in hints:
                if self._norm(hint) in hh:
                    return True
        return False

    def _cell_to_str(self, value: Any) -> str:
        if value is None:
            return ""
        try:
            return str(value).strip()
        except Exception:
            return ""

    def _normalize_headers(self, headers: List[str]) -> List[str]:
        """
        Normaliza encabezados:
        - strip
        - lower
        - colapsa espacios
        - elimina acentos (para que Número == Numero)
        """
        out: List[str] = []
        for h in headers:
            h2 = " ".join((h or "").strip().lower().split())
            h2 = self._strip_accents(h2)
            out.append(h2)
        return out

    def _strip_accents(self, s: str) -> str:
        if not s:
            return ""
        return "".join(
            c for c in unicodedata.normalize("NFKD", s)
            if not unicodedata.combining(c)
        )

    def _norm(self, s: str) -> str:
        return self._strip_accents(" ".join((s or "").strip().lower().split()))

    def _is_mostly_empty(self, row: List[str]) -> bool:
        if not row:
            return True
        non_empty = sum(1 for c in row if c and c.strip())
        return non_empty <= max(1, int(len(row) * 0.05))  # <=5% con contenido