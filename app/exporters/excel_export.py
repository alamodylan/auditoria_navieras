# app/exporters/excel_export.py

import os
import pandas as pd

from app.extensions import db
from app.models import ResultSummary, ResultContainer, ResultCharge, ResultException, ResultKPI

def export_job_to_excel(job_id: int, output_folder: str) -> str:
    """
    Genera outputs/<job_id>/Auditoria_<job_id>.xlsx con multihoja:
      Resumen_Guias, Detalle_Contenedores, Detalle_Cargos, Excepciones, KPIs
    """
    out_dir = os.path.join(output_folder, str(job_id))
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"Auditoria_{job_id}.xlsx")

    # Query results
    resumen = ResultSummary.query.filter_by(job_id=job_id).all()
    conts = ResultContainer.query.filter_by(job_id=job_id).all()
    cargos = ResultCharge.query.filter_by(job_id=job_id).all()
    excs = ResultException.query.filter_by(job_id=job_id).all()
    kpi = ResultKPI.query.filter_by(job_id=job_id).first()

    df_resumen = pd.DataFrame([{
        "Guía": r.guia,
        "Estado": r.estado,
        "Total FILS": float(r.total_fils),
        "Total Naviera": float(r.total_naviera),
        "Diferencia": float(r.diferencia),
        "OK": bool(r.ok),
        "Naviera": r.naviera,
        "Fuente Naviera": r.fuente_naviera or "",
    } for r in resumen])

    df_conts = pd.DataFrame([{
        "Guía": c.guia,
        "Contenedor": c.contenedor,
        "Ruta": c.ruta or "",
        "Flete": float(c.flete),
        "Extras": float(c.extras),
        "Total": float(c.total),
        "Naviera": c.naviera,
    } for c in conts])

    df_cargos = pd.DataFrame([{
        "Guía": ch.guia,
        "Contenedor": ch.contenedor or "",
        "Tipo Cargo": ch.tipo_cargo,
        "Monto": float(ch.monto),
        "Origen": ch.origen,
        "Naviera": ch.naviera,
    } for ch in cargos])

    df_excs = pd.DataFrame([{
        "Tipo": e.tipo,
        "Guía": e.guia or "",
        "Contenedor": e.contenedor or "",
        "Severidad": e.severidad,
        "Detalle": e.detalle,
        "Naviera": e.naviera,
    } for e in excs])

    df_kpi = pd.DataFrame([{
        "Naviera": kpi.naviera if kpi else "",
        "Total Guías": kpi.total_guias if kpi else 0,
        "Guías OK": kpi.guias_ok if kpi else 0,
        "Guías con Diferencia": kpi.guias_diferencia if kpi else 0,
        "Guías NO_CERRADA": kpi.guias_no_cerrada if kpi else 0,
        "Solo en FILS": kpi.guias_solo_en_fils if kpi else 0,
        "Solo en Naviera": kpi.guias_solo_en_naviera if kpi else 0,
        "Total FILS": float(kpi.total_fils) if kpi else 0,
        "Total Naviera": float(kpi.total_naviera) if kpi else 0,
        "Diferencia Global": float(kpi.diferencia_global) if kpi else 0,
    }])

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_resumen.to_excel(writer, sheet_name="Resumen_Guias", index=False)
        df_conts.to_excel(writer, sheet_name="Detalle_Contenedores", index=False)
        df_cargos.to_excel(writer, sheet_name="Detalle_Cargos", index=False)
        df_excs.to_excel(writer, sheet_name="Excepciones", index=False)
        df_kpi.to_excel(writer, sheet_name="KPIs", index=False)

    return out_path