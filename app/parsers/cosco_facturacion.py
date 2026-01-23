# app/parsers/cosco_facturacion.py

from typing import Dict, List
import pandas as pd

from app.parsers.base import BaseParser
from app.parsers.normalization import map_columns_by_synonyms, normalize_guia, normalize_contenedor, normalize_amount

class COSCOFacturacionParser(BaseParser):
    """
    COSCO multihoja. Consolidamos todas las hojas.
    Campos normalizados:
      guia, contenedor (si viene), total_naviera, ruta, sheet
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
        meta = {"errors": [], "warnings": []}
        try:
            xls = pd.ExcelFile(path)
            meta["sheets"] = xls.sheet_names
            if not xls.sheet_names:
                meta["errors"].append("COSCO: el archivo no contiene hojas.")
                return meta

            # probar 1ra hoja con datos
            df = pd.read_excel(path, sheet_name=xls.sheet_names[0], nrows=5)
            mapped = map_columns_by_synonyms(list(df.columns), self.SYNONYMS)

            if not mapped["guia"]:
                meta["errors"].append("COSCO: no se encontró columna Documento/Guía en la primera hoja (puede variar por hoja).")
            if not mapped["total"]:
                meta["errors"].append("COSCO: no se encontró columna Total/Monto en la primera hoja (puede variar por hoja).")

            meta["mapped_sample"] = mapped
        except Exception as e:
            meta["errors"].append(f"COSCO: no se pudo leer el archivo: {e}")
        return meta

    def parse(self, path: str) -> List[dict]:
        xls = pd.ExcelFile(path)
        rows: List[dict] = []

        for sheet in xls.sheet_names:
            df = pd.read_excel(path, sheet_name=sheet)
            if df.empty:
                continue

            mapped = map_columns_by_synonyms(list(df.columns), self.SYNONYMS)
            guia_col = mapped["guia"]
            total_col = mapped["total"]

            # Si una hoja no trae lo mínimo, la ignoramos (pero en producción podés registrar warning)
            if not guia_col or not total_col:
                continue

            cont_col = mapped["contenedor"]
            ruta_col = mapped["ruta"]
            predio_col = mapped["predio"]

            for _, r in df.iterrows():
                guia = normalize_guia(r.get(guia_col))
                if not guia:
                    continue

                total_nav = normalize_amount(r.get(total_col))
                cont = normalize_contenedor(r.get(cont_col)) if cont_col else ""
                ruta = str(r.get(ruta_col) or "").strip() if ruta_col else ""
                predio = str(r.get(predio_col) or "").strip() if predio_col else ""

                rows.append({
                    "guia": guia,
                    "contenedor": cont,
                    "total_naviera": total_nav or 0,
                    "ruta": ruta,
                    "predio": predio,
                    "sheet": sheet,
                })

        return rows