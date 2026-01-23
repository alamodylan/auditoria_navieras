# app/parsers/one_facturacion.py

from typing import Dict, List
import pandas as pd

from app.parsers.base import BaseParser
from app.parsers.normalization import map_columns_by_synonyms, normalize_guia, normalize_contenedor, normalize_amount

class ONEFacturacionParser(BaseParser):
    """
    ONE puede variar mucho. MVP: toma una hoja principal, busca columnas por sinónimos.
    """

    SYNONYMS = {
        "guia": ["Guia", "Guía", "Documento", "No Documento", "Referencia", "Reference"],
        "contenedor": ["Contenedor", "Container", "CNTR"],
        "total": ["Total", "Monto", "Importe", "Amount", "Total Facturado"],
        "ruta": ["Ruta", "Servicio", "Service", "Tipo", "Servicio Facturado"],
    }

    def sniff(self, path: str) -> Dict:
        meta = {"errors": [], "warnings": []}
        try:
            xls = pd.ExcelFile(path)
            meta["sheets"] = xls.sheet_names
            df = pd.read_excel(path, sheet_name=xls.sheet_names[0], nrows=5)
            mapped = map_columns_by_synonyms(list(df.columns), self.SYNONYMS)

            if not mapped["guia"]:
                meta["errors"].append("ONE: no se encontró columna Guía/Documento/Referencia.")
            if not mapped["total"]:
                meta["errors"].append("ONE: no se encontró columna Total/Monto.")

            meta["mapped_sample"] = mapped
        except Exception as e:
            meta["errors"].append(f"ONE: no se pudo leer el archivo: {e}")
        return meta

    def parse(self, path: str) -> List[dict]:
        xls = pd.ExcelFile(path)
        df = pd.read_excel(path, sheet_name=xls.sheet_names[0])
        mapped = map_columns_by_synonyms(list(df.columns), self.SYNONYMS)

        guia_col = mapped["guia"]
        total_col = mapped["total"]
        if not guia_col or not total_col:
            raise ValueError("ONE: columnas mínimas no encontradas (guía/total).")

        cont_col = mapped["contenedor"]
        ruta_col = mapped["ruta"]

        rows: List[dict] = []
        for _, r in df.iterrows():
            guia = normalize_guia(r.get(guia_col))
            if not guia:
                continue

            rows.append({
                "guia": guia,
                "contenedor": normalize_contenedor(r.get(cont_col)) if cont_col else "",
                "total_naviera": normalize_amount(r.get(total_col)) or 0,
                "ruta": str(r.get(ruta_col) or "").strip() if ruta_col else "",
                "sheet": xls.sheet_names[0],
            })
        return rows