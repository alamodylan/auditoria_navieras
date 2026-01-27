# app/services/job_runner.py

from __future__ import annotations

from decimal import Decimal
from typing import Dict, List, Iterable

from app.extensions import db
from app.models import (
    Job,
    ResultSummary, ResultContainer, ResultCharge, ResultException, ResultKPI
)

from app.parsers.fils_auditoria import FILSAuditoriaParser
from app.parsers.cosco_facturacion import COSCOFacturacionParser
from app.parsers.one_facturacion import ONEFacturacionParser

from app.services.reconciliation import reconcile
from app.services.kpis import compute_kpis
from app.exporters.excel_export import export_job_to_excel

from app.utils.logging import get_logger

logger = get_logger("job_runner")

BATCH_SIZE = 1000


def _bulk_delete_job_results(job_id: int) -> None:
    ResultSummary.query.filter_by(job_id=job_id).delete(synchronize_session=False)
    ResultContainer.query.filter_by(job_id=job_id).delete(synchronize_session=False)
    ResultCharge.query.filter_by(job_id=job_id).delete(synchronize_session=False)
    ResultException.query.filter_by(job_id=job_id).delete(synchronize_session=False)
    ResultKPI.query.filter_by(job_id=job_id).delete(synchronize_session=False)
    db.session.commit()


def _bulk_insert(model, rows: List[dict]) -> None:
    if not rows:
        return
    db.session.bulk_insert_mappings(model, rows)
    db.session.commit()


def run_job(job_id: int, money_tolerance: float, output_folder: str) -> Dict:
    job = Job.query.get(job_id)
    if not job:
        raise ValueError(f"Job no existe: {job_id}")

    job.mark_running()
    db.session.commit()

    try:
        # ----------------------------
        # obtener paths
        # ----------------------------
        files = {f.file_type.upper(): f for f in job.files}
        if "FILS" not in files:
            raise ValueError("Falta archivo FILS en el Job.")
        if job.naviera.upper() not in files:
            raise ValueError(f"Falta archivo de facturación {job.naviera} en el Job.")

        fils_path = files["FILS"].stored_path
        fact_path = files[job.naviera.upper()].stored_path
        tol = Decimal(str(money_tolerance))

        # ============================================================
        # 1) Parse FILS COMPLETO (Guía + Contenedor + Cargos Adicionales)
        #    -> aquí ya viene contenedor ligado por guía y cargos filtrados
        # ============================================================
        fils_parser = FILSAuditoriaParser()
        fils_rows: List[dict] = fils_parser.parse(fils_path)

        logger.info(f"FILS parsed rows={len(fils_rows)} (Guía + Contenedor + Cargos Adicionales)")

        # ============================================================
        # 2) Parse naviera (COSCO / ONE)
        # ============================================================
        if job.naviera.upper() == "COSCO":
            nav_parser = COSCOFacturacionParser()
        else:
            nav_parser = ONEFacturacionParser()

        nav_rows: List[dict] = list(nav_parser.parse(fact_path))
        logger.info(f"{job.naviera.upper()} parsed rows={len(nav_rows)}")

        # ============================================================
        # 3) Reconcile
        # ============================================================
        resumen, det_cont, det_cargos, excs = reconcile(
            job.naviera,
            fils_rows=fils_rows,
            naviera_rows=nav_rows,
            money_tolerance=tol
        )

        # ============================================================
        # 4) Persist results
        # ============================================================
        _bulk_delete_job_results(job_id)

        # Summary bulk
        buf: List[dict] = []
        for r in resumen:
            buf.append({
                "job_id": job_id,
                "guia": r.guia,
                "estado": r.estado,
                "total_fils": r.total_fils,
                "total_naviera": r.total_naviera,
                "diferencia": r.diferencia,
                "ok": r.ok,
                "naviera": r.naviera,
                "fuente_naviera": r.fuente_naviera,
            })
            if len(buf) >= BATCH_SIZE:
                _bulk_insert(ResultSummary, buf)
                buf.clear()
        _bulk_insert(ResultSummary, buf)

        # Containers bulk
        buf = []
        for c in det_cont:
            buf.append({
                "job_id": job_id,
                "guia": str(c.get("guia", "")),
                "contenedor": str(c.get("contenedor", "")),
                "ruta": str(c.get("ruta", "")),
                "flete": c.get("flete") or 0,
                "extras": c.get("extras") or 0,
                "total": c.get("total") or 0,
                "naviera": str(c.get("naviera", "")),
            })
            if len(buf) >= BATCH_SIZE:
                _bulk_insert(ResultContainer, buf)
                buf.clear()
        _bulk_insert(ResultContainer, buf)

        # Charges bulk (incluye cargos adicionales del FILS)
        buf = []
        for ch in det_cargos:
            buf.append({
                "job_id": job_id,
                "guia": str(ch.get("guia", "")),
                "contenedor": str(ch.get("contenedor", "")),
                "tipo_cargo": str(ch.get("tipo_cargo", "CARGO")),
                "monto": ch.get("monto") or 0,
                "origen": str(ch.get("origen", "FILS")),
                "naviera": str(ch.get("naviera", "")),
            })
            if len(buf) >= BATCH_SIZE:
                _bulk_insert(ResultCharge, buf)
                buf.clear()
        _bulk_insert(ResultCharge, buf)

        # Exceptions bulk
        buf = []
        for e in excs:
            buf.append({
                "job_id": job_id,
                "tipo": e.tipo,
                "guia": e.guia,
                "contenedor": e.contenedor,
                "detalle": e.detalle,
                "severidad": e.severidad,
                "naviera": e.naviera,
            })
            if len(buf) >= BATCH_SIZE:
                _bulk_insert(ResultException, buf)
                buf.clear()
        _bulk_insert(ResultException, buf)

        # KPI
        resumen_dicts = [{
            "guia": r.guia,
            "ok": r.ok,
            "estado": r.estado,
            "total_fils": str(r.total_fils),
            "total_naviera": str(r.total_naviera),
            "diferencia": str(r.diferencia),
        } for r in resumen]

        kpi = compute_kpis(job.naviera.upper(), resumen_dicts)

        db.session.add(ResultKPI(
            job_id=job_id,
            naviera=kpi["naviera"],
            total_guias=kpi["total_guias"],
            guias_ok=kpi["guias_ok"],
            guias_diferencia=kpi["guias_diferencia"],
            guias_no_cerrada=kpi["guias_no_cerrada"],
            guias_solo_en_fils=kpi["guias_solo_en_fils"],
            guias_solo_en_naviera=kpi["guias_solo_en_naviera"],
            total_fils=kpi["total_fils"],
            total_naviera=kpi["total_naviera"],
            diferencia_global=kpi["diferencia_global"],
        ))
        db.session.commit()

        # Export
        export_path = export_job_to_excel(job_id=job_id, output_folder=output_folder)

        job.mark_done()
        db.session.commit()

        return {
            "job_id": job_id,
            "status": "DONE",
            "export_path": export_path,
            "kpi": kpi,
        }

    except Exception as e:
        job.mark_failed(e)
        db.session.commit()
        logger.exception(f"Job failed id={job_id}: {e}")
        return {"job_id": job_id, "status": "FAILED", "error": str(e)}



