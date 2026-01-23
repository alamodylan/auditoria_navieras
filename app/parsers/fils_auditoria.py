# app/parsers/fils_auditoria.py

from typing import Dict, List
import pandas as pd

from app.parsers.base import BaseParser
from app.parsers.normalization import map_columns_by_synonyms, normalize_guia, normalize_contenedor, normalize_amount
from app.utils.dates import parse_datetime

class FILSAuditoriaParser(BaseParser):
    """
    Parser del ReporteGuiaAuditoria*.xlsx (FILS).
    Produce filas por guía (última cerrada se elige luego en conciliación).
    """

    SYNONYMS = {
        "guia": ["Guia", "Guía", "No Guia", "N° Guia", "Documento", "No. Documento"],
        "contenedor": ["Contenedor", "Container", "CNTR"],
        "estado": ["Estado", "Estado Guia", "Estado Guía", "Estado de Guía"],
        "fecha_cierre": ["Fecha Cierre", "Fecha de Cierre", "Fecha Hora Cierre", "Fecha y Hora Cierre"],
        "fecha": ["Fecha", "Fecha Movimiento", "Fecha y Hora", "Fecha Hora"],
        "monto_tarifa": ["Monto Tarifa", "Tarifa", "Monto", "Total", "Monto Total"],
        "monto_flete": ["Flete", "Monto Flete", "Costo Flete", "Tarifa Flete"],
        "tipo_cargo": ["Tipo", "Tipo Cargo", "Cargo", "Descripcion", "Descripción", "Tipo Servicio"],
        "monto_cargo": ["Monto Cargo", "Monto", "Total", "Importe"],
        "ruta": ["Ruta", "Ruta Tipo", "Servicio", "Tipo Servicio", "Servicio Facturado"],
        "naviera": ["Naviera", "Shipping Line", "Linea Naviera", "Línea Naviera"],
    }

    def sniff(self, path: str) -> Dict:
        meta = {"errors": [], "warnings": []}
        try:
            df = pd.read_excel(path, sheet_name=0, nrows=5)
            mapped = map_columns_by_synonyms(list(df.columns), self.SYNONYMS)

            # mínimos
            if not mapped["guia"]:
                meta["errors"].append("FILS: no se encontró columna de Guía/Documento.")
            if not mapped["contenedor"]:
                meta["warnings"].append("FILS: no se encontró columna Contenedor (se puede seguir, pero afecta detalle).")

            meta["mapped_columns"] = mapped
            meta["sheet"] = 0
        except Exception as e:
            meta["errors"].append(f"FILS: no se pudo leer el archivo: {e}")
        return meta

    def parse(self, path: str) -> List[dict]:
        df = pd.read_excel(path, sheet_name=0)
        mapped = map_columns_by_synonyms(list(df.columns), self.SYNONYMS)

        guia_col = mapped["guia"]
        if not guia_col:
            raise ValueError("FILS: columna Guía no encontrada.")

        cont_col = mapped["contenedor"]
        estado_col = mapped["estado"]
        fecha_cierre_col = mapped["fecha_cierre"]
        fecha_col = mapped["fecha"]
        tarifa_col = mapped["monto_tarifa"]
        flete_col = mapped["monto_flete"]
        tipo_cargo_col = mapped["tipo_cargo"]
        monto_cargo_col = mapped["monto_cargo"]
        ruta_col = mapped["ruta"]
        naviera_col = mapped["naviera"]

        rows: List[dict] = []
        for _, r in df.iterrows():
            guia = normalize_guia(r.get(guia_col))
            if not guia:
                continue

            contenedor = normalize_contenedor(r.get(cont_col)) if cont_col else ""
            estado = str(r.get(estado_col) or "").strip().upper() if estado_col else ""
            fecha_cierre = parse_datetime(r.get(fecha_cierre_col)) if fecha_cierre_col else None
            fecha = parse_datetime(r.get(fecha_col)) if fecha_col else None

            # montos
            monto_total = normalize_amount(r.get(tarifa_col)) if tarifa_col else None
            monto_flete = normalize_amount(r.get(flete_col)) if flete_col else None

            # Cargos: si el archivo viene “una fila por cargo”, guardamos lista de cargos.
            # Para MVP: agregamos cada fila como cargo si existe tipo/monto.
            cargos = []
            if tipo_cargo_col and monto_cargo_col:
                tipo = str(r.get(tipo_cargo_col) or "").strip()
                monto = normalize_amount(r.get(monto_cargo_col))
                if tipo and monto is not None:
                    cargos.append({"tipo_cargo": tipo, "monto": monto, "contenedor": contenedor})

            ruta = str(r.get(ruta_col) or "").strip() if ruta_col else ""
            naviera = str(r.get(naviera_col) or "").strip() if naviera_col else ""

            # Extras: suma de cargos que no sean flete (MVP: si tipo contiene "ADIC" o no contiene "FLETE")
            monto_extras = 0
            for c in cargos:
                t = str(c.get("tipo_cargo","")).upper()
                if "FLETE" not in t:
                    monto_extras += c.get("monto") or 0

            rows.append({
                "guia": guia,
                "contenedor": contenedor,
                "estado": estado,
                "fecha_cierre": fecha_cierre,
                "fecha": fecha,
                "monto_total": monto_total or 0,
                "monto_flete": monto_flete or 0,
                "monto_extras": monto_extras or 0,
                "ruta": ruta,
                "naviera": naviera,
                "cargos": cargos,
            })

        return rows