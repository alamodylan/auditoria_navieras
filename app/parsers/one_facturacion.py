# app/parsers/one_facturacion.py

from typing import Dict, List, Optional
import pandas as pd

from app.parsers.base import BaseParser
from app.parsers.normalization import (
    map_columns_by_synonyms,
    normalize_guia,
    normalize_contenedor,
    normalize_amount,
)

class ONEFacturacionParser(BaseParser):
    """
    ONE puede variar mucho.

    Importante:
    - A veces trae Guía, a veces NO.
    - Si no trae Guía, se debe poder reconciliar por Contenedor.
    """

    SYNONYMS = {
        "guia": ["Guia", "Guía", "Documento", "No Documento", "Referencia", "Reference"],
        "contenedor": ["Contenedor", "Container", "CNTR"],
        "total": ["Total", "Monto", "Importe", "Amount", "Total Facturado"],
        "ruta": ["Ruta", "Servicio", "Service", "Tipo", "Servicio Facturado"],
        "fecha": ["Fecha", "Date", "Fecha Movimiento"],
        "tipo_cargo": ["Cargo", "Tipo Cargo", "Concepto", "Charge", "Charge Type"],
    }

    def sniff(self, path: str) -> Dict:
        meta = {"errors": [], "warnings": []}
        try:
            xls = pd.ExcelFile(path)
            meta["sheets"] = xls.sheet_names

            sheet0 = xls.sheet_names[0]
            df = pd.read_excel(path, sheet_name=sheet0, nrows=5)

            mapped = map_columns_by_synonyms(list(df.columns), self.SYNONYMS)

            # Guía en ONE NO siempre existe -> warning, NO error
            if not mapped["guia"]:
                meta["warnings"].append("ONE: no se encontró columna Guía/Documento/Referencia. Se reconciliará por Contenedor.")
            if not mapped["contenedor"]:
                meta["errors"].append("ONE: no se encontró columna Contenedor/Container (necesaria si no hay guía).")
            if not mapped["total"]:
                meta["errors"].append("ONE: no se encontró columna Total/Monto (obligatoria).")

            meta["mapped_sample"] = mapped
            meta["sheet_used"] = sheet0
            meta["headers_preview"] = list(df.columns)[:50]

        except Exception as e:
            meta["errors"].append(f"ONE: no se pudo leer el archivo: {e}")
        return meta

    def parse(self, path: str) -> List[dict]:
        xls = pd.ExcelFile(path)
        sheet0 = xls.sheet_names[0]
        df = pd.read_excel(path, sheet_name=sheet0)

        mapped = map_columns_by_synonyms(list(df.columns), self.SYNONYMS)

        guia_col = mapped["guia"]              # puede ser None
        cont_col = mapped["contenedor"]        # requerido en la práctica
        total_col = mapped["total"]            # requerido
        ruta_col = mapped["ruta"]
        fecha_col = mapped["fecha"]
        cargo_col = mapped["tipo_cargo"]

        if not total_col:
            raise ValueError("ONE: columna Total/Monto no encontrada.")
        if not cont_col and not guia_col:
            raise ValueError("ONE: no hay Contenedor ni Guía; no se puede reconciliar.")

        rows: List[dict] = []
        for _, r in df.iterrows():
            guia = normalize_guia(r.get(guia_col)) if guia_col else ""
            cont = normalize_contenedor(r.get(cont_col)) if cont_col else ""

            # si no hay guía y no hay contenedor en la fila, no sirve para auditar
            if not guia and not cont:
                continue

            total_nav = normalize_amount(r.get(total_col)) or 0

            rows.append({
                "guia": guia,  # puede venir ""
                "contenedor": cont,  # clave cuando no hay guía
                "total_naviera": total_nav,
                "ruta": str(r.get(ruta_col) or "").strip() if ruta_col else "",
                "fecha": r.get(fecha_col) if fecha_col else None,
                "tipo_cargo": str(r.get(cargo_col) or "").strip() if cargo_col else "",
                "sheet": sheet0,
            })

        return rows
