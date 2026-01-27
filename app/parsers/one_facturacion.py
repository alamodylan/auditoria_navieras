from typing import Dict, List
import pandas as pd

from app.parsers.base import BaseParser
from app.parsers.normalization import (
    map_columns_by_synonyms, normalize_guia, normalize_contenedor, normalize_amount
)

class ONEFacturacionParser(BaseParser):
    """
    ONE: puede venir SIN GUIA. En ese caso se cruza por contenedor.
    """

    SYNONYMS = {
        "guia": ["Guia", "Guía", "Documento", "No Documento", "Referencia", "Reference"],
        "contenedor": ["Contenedor", "Container", "CNTR"],
        "total": ["Total", "Monto", "Importe", "Amount", "Total Facturado", "Total Naviera"],
        "ruta": ["Ruta", "Servicio", "Service", "Tipo", "Servicio Facturado"],
        "cargo": ["Cargo", "Concepto", "Detalle", "Descripción", "Descripcion"],
    }

    def sniff(self, path: str) -> Dict:
        meta = {"errors": [], "warnings": []}
        try:
            xls = pd.ExcelFile(path)
            meta["sheets"] = xls.sheet_names
            sheet = xls.sheet_names[0]
            meta["sheet_used"] = sheet

            df = pd.read_excel(path, sheet_name=sheet, nrows=5)
            mapped = map_columns_by_synonyms(list(df.columns), self.SYNONYMS)
            meta["mapped_sample"] = mapped
            meta["headers_preview"] = list(df.columns)[:40]

            # total es obligatorio
            if not mapped["total"]:
                meta["errors"].append("ONE: no se encontró columna Total/Monto/Importe.")

            # si no hay guía, contenedor debe existir
            if not mapped["guia"] and not mapped["contenedor"]:
                meta["errors"].append("ONE: no se encontró ni Guía ni Contenedor (uno debe existir).")

            # aviso si no hay guía
            if not mapped["guia"]:
                meta["warnings"].append("ONE: no trae Guía. Se cruzará por Contenedor.")

        except Exception as e:
            meta["errors"].append(f"ONE: no se pudo leer el archivo: {e}")
        return meta

    def parse(self, path: str) -> List[dict]:
        xls = pd.ExcelFile(path)
        sheet = xls.sheet_names[0]
        df = pd.read_excel(path, sheet_name=sheet)

        mapped = map_columns_by_synonyms(list(df.columns), self.SYNONYMS)

        guia_col = mapped["guia"]
        cont_col = mapped["contenedor"]
        total_col = mapped["total"]
        ruta_col = mapped["ruta"]
        cargo_col = mapped["cargo"]

        if not total_col:
            raise ValueError("ONE: columna Total/Monto no encontrada.")
        if not guia_col and not cont_col:
            raise ValueError("ONE: no hay columna Guía ni Contenedor para cruzar.")

        rows: List[dict] = []
        for _, r in df.iterrows():
            guia = normalize_guia(r.get(guia_col)) if guia_col else ""
            cont = normalize_contenedor(r.get(cont_col)) if cont_col else ""

            # Si no hay guía, entonces contenedor es obligatorio
            if not guia and not cont:
                continue

            rows.append({
                "guia": guia,  # puede ir vacío
                "contenedor": cont,
                "total_naviera": normalize_amount(r.get(total_col)) or 0,
                "ruta": str(r.get(ruta_col) or "").strip() if ruta_col else "",
                "cargo": str(r.get(cargo_col) or "").strip() if cargo_col else "",
                "sheet": sheet,
            })
        return rows