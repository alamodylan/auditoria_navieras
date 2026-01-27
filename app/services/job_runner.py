# app/services/job_runner.py

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Iterable, Optional, Tuple, Any, DefaultDict
from collections import defaultdict

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
from app.utils.money import parse_money

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


def _parse_fecha(value) -> Optional[datetime]:
    """
    Parsea fecha del FILS.
    Puede venir como datetime (openpyxl) o string tipo: 01/10/2025 08:49
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value

    s = str(value).strip()
    if not s:
        return None

    # formato común del reporte FILS
    try:
        return datetime.strptime(s, "%d/%m/%Y %H:%M")
    except Exception:
        return None


def _norm_contenedor(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip().upper()
    return s.replace("-", "").replace(" ", "")


def _norm_accion(value: Any) -> str:
    # normaliza para detectar "ELIMINADO"
    if value is None:
        return ""
    return str(value).strip().upper()


def _find_idx(headers: List[str], hints: List[str]) -> Optional[int]:
    # headers vienen normalizados a lower por FILSAuditoriaParser.iter_rows()
    for i, h in enumerate(headers):
        hh = (h or "")
        for hint in hints:
            if hint in hh:
                return i
    return None


def _cell(row: Tuple[Any, ...], i: Optional[int]) -> Any:
    if i is None:
        return None
    return row[i] if i < len(row) else None


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

        fils_parser = FILSAuditoriaParser()

        # ============================================================
        # 1) Construir map: guia -> contenedor usando hoja "Contenedor"
        # ============================================================
        # Headers esperados (variaciones):
        cont_guia_hints = ["número guía", "numero guía", "numero guia"]
        cont_cont_hints = ["contenedor"]

        guia_to_contenedor: Dict[str, str] = {}

        # Nota: tu XLSX tiene tabs: Guía, Chasis, Contenedor, Equipo, Cargos Adicionales, Combustible
        # Leemos explícitamente la hoja "Contenedor"
        cont_sheet_row = 1  # header row
        for headers, row in fils_parser.iter_rows(fils_path, sheet_name="Contenedor", header_row=cont_sheet_row):
            if not headers:
                continue

            idx_guia = _find_idx(headers, cont_guia_hints)
            idx_cont = _find_idx(headers, cont_cont_hints)

            if idx_guia is None:
                raise ValueError("FILS/Contenedor: no se encontró columna 'Número Guía'.")
            if idx_cont is None:
                raise ValueError("FILS/Contenedor: no se encontró columna 'Contenedor'.")

            # una vez fijados índices, recorremos datos usando el mismo iterador
            # (pero como iter_rows ya está entregando fila por fila, aplicamos en el loop)
            # IMPORTANTE: para no recalcular idx en cada fila, rompemos y re-iteramos con idx fijos.
            break

        # Re-iteramos ya con índices fijos (sin recalcular cada vez)
        for headers, row in fils_parser.iter_rows(fils_path, sheet_name="Contenedor", header_row=cont_sheet_row):
            if not headers:
                continue
            idx_guia = _find_idx(headers, cont_guia_hints)
            idx_cont = _find_idx(headers, cont_cont_hints)
            if idx_guia is None or idx_cont is None:
                # si esta hoja viene rara, mejor fallar claro
                raise ValueError("FILS/Contenedor: encabezados inválidos; no se pudieron mapear guía/contenedor.")

            guia = str(_cell(row, idx_guia) or "").strip()
            if not guia:
                continue
            cont = _norm_contenedor(_cell(row, idx_cont))
            if not cont:
                continue

            # si hay repetidos, normalmente nos sirve el último por “aparición”;
            # si quisieras "último por fecha" también se puede (pero en contenedor normalmente no hace falta).
            guia_to_contenedor[guia] = cont

        logger.info(f"FILS Contenedor: guias_mapeadas={len(guia_to_contenedor)}")

        # ============================================================
        # 2) Cargos Adicionales: quedarnos con el ÚLTIMO por (guia, cargo)
        #    y EXCLUIR si el último termina en ACCIÓN=ELIMINADO
        # ============================================================
        cargos_sheet = "Cargos Adicionales"
        ca_guia_hints = ["número guía", "numero guía", "numero guia"]
        ca_cargo_hints = ["cargo"]           # "Cargo" (texto)
        ca_monto_hints = ["monto naviera"]   # monto para comparar
        ca_fecha_hints = ["fecha"]
        ca_accion_hints = ["acción", "accion"]

        # estructura: latest[(guia, cargo)] = {"fecha": dt, "accion": str, "monto": Decimal, "raw": {...}}
        latest_ca: Dict[Tuple[str, str], Dict[str, Any]] = {}

        # Detectar índices 1 vez
        for headers, row in fils_parser.iter_rows(fils_path, sheet_name=cargos_sheet, header_row=1):
            if not headers:
                continue

            idx_g = _find_idx(headers, ca_guia_hints)
            idx_c = _find_idx(headers, ca_cargo_hints)
            idx_m = _find_idx(headers, ca_monto_hints)
            idx_f = _find_idx(headers, ca_fecha_hints)
            idx_a = _find_idx(headers, ca_accion_hints)

            if idx_g is None:
                raise ValueError("FILS/Cargos Adicionales: no se encontró columna 'Número Guía'.")
            if idx_c is None:
                raise ValueError("FILS/Cargos Adicionales: no se encontró columna 'Cargo'.")
            if idx_m is None:
                raise ValueError("FILS/Cargos Adicionales: no se encontró columna 'Monto Naviera'.")
            if idx_f is None:
                # sin fecha no podemos definir "último"; preferimos fallar explícito
                raise ValueError("FILS/Cargos Adicionales: no se encontró columna 'Fecha' para determinar el último registro.")
            if idx_a is None:
                raise ValueError("FILS/Cargos Adicionales: no se encontró columna 'Acción' para filtrar eliminados.")

            break

        # Re-iterar con lógica de "último"
        for headers, row in fils_parser.iter_rows(fils_path, sheet_name=cargos_sheet, header_row=1):
            if not headers:
                continue

            idx_g = _find_idx(headers, ca_guia_hints)
            idx_c = _find_idx(headers, ca_cargo_hints)
            idx_m = _find_idx(headers, ca_monto_hints)
            idx_f = _find_idx(headers, ca_fecha_hints)
            idx_a = _find_idx(headers, ca_accion_hints)

            guia = str(_cell(row, idx_g) or "").strip()
            if not guia:
                continue

            cargo = str(_cell(row, idx_c) or "").strip()
            if not cargo:
                continue

            fecha = _parse_fecha(_cell(row, idx_f))
            accion = _norm_accion(_cell(row, idx_a))
            monto = parse_money(_cell(row, idx_m))

            key = (guia, cargo.upper())  # cargo en mayúscula para agrupar
            prev = latest_ca.get(key)

            # si no hay fecha, lo tratamos como antiguo (pero ya fallamos arriba si falta la col)
            if prev is None:
                latest_ca[key] = {"fecha": fecha, "accion": accion, "monto": monto, "cargo": cargo}
            else:
                # comparar por fecha; si empatan, el último que aparece gana
                prev_dt = prev.get("fecha")
                if prev_dt is None or (fecha is not None and fecha >= prev_dt):
                    latest_ca[key] = {"fecha": fecha, "accion": accion, "monto": monto, "cargo": cargo}

        # Filtrar eliminados
        cargos_por_guia: DefaultDict[str, List[dict]] = defaultdict(list)
        eliminados = 0
        for (guia, cargo_key), info in latest_ca.items():
            accion = info.get("accion", "")
            if "ELIMIN" in accion:  # ELIMINADO / ELIMINAR / ELIMINACIÓN...
                eliminados += 1
                continue
            cargos_por_guia[guia].append({
                "tipo_cargo": info.get("cargo") or cargo_key,
                "monto": str(info.get("monto") or Decimal("0")),
                "contenedor": guia_to_contenedor.get(guia, ""),
            })

        logger.info(
            f"FILS CargosAdicionales: claves={len(latest_ca)} activos={sum(len(v) for v in cargos_por_guia.values())} eliminados={eliminados}"
        )

        # ============================================================
        # 3) Hoja "Guía": dataset base. Aquí NO exigimos contenedor,
        #    porque lo pegamos desde hoja Contenedor.
        # ============================================================
        guia_sheet = "Guía"
        g_guia_hints = ["número guía", "numero guía", "numero guia"]
        g_estado_hints = ["estado"]
        g_fecha_hints = ["fecha"]
        g_ruta_hints = ["ruta"]
        g_monto_tarifa_hints = ["monto tarifa"]  # este es el total FILS a comparar

        # índices 1 vez
        for headers, row in fils_parser.iter_rows(fils_path, sheet_name=guia_sheet, header_row=1):
            if not headers:
                continue

            idx_guia = _find_idx(headers, g_guia_hints)
            idx_estado = _find_idx(headers, g_estado_hints)
            idx_fecha = _find_idx(headers, g_fecha_hints)
            idx_ruta = _find_idx(headers, g_ruta_hints)
            idx_monto = _find_idx(headers, g_monto_tarifa_hints)

            if idx_guia is None:
                raise ValueError("FILS/Guía: no se encontró columna 'Número Guía'.")
            if idx_monto is None:
                raise ValueError("FILS/Guía: no se encontró columna 'Monto Tarifa' (total a comparar).")

            break

        def iter_fils_dicts() -> Iterable[dict]:
            for headers, row in fils_parser.iter_rows(fils_path, sheet_name=guia_sheet, header_row=1):
                if not headers:
                    continue

                idx_guia = _find_idx(headers, g_guia_hints)
                idx_estado = _find_idx(headers, g_estado_hints)
                idx_fecha = _find_idx(headers, g_fecha_hints)
                idx_ruta = _find_idx(headers, g_ruta_hints)
                idx_monto = _find_idx(headers, g_monto_tarifa_hints)

                guia = str(_cell(row, idx_guia) or "").strip()
                if not guia:
                    continue

                estado = str(_cell(row, idx_estado) or "").strip().upper() if idx_estado is not None else ""
                fecha = _parse_fecha(_cell(row, idx_fecha)) if idx_fecha is not None else None
                ruta = str(_cell(row, idx_ruta) or "").strip() if idx_ruta is not None else ""
                monto_tarifa = _cell(row, idx_monto)

                cont = guia_to_contenedor.get(guia, "")
                cont = _norm_contenedor(cont)

                yield {
                    "guia": guia,
                    "contenedor": cont,          # <- viene de hoja Contenedor (puede estar vacío si no existe)
                    "estado": estado,
                    "fecha": fecha,
                    "fecha_cierre": fecha,       # para "último" en reconcile
                    "monto_total": monto_tarifa, # <- total FILS para comparar
                    "monto_flete": None,
                    "monto_extras": None,
                    "ruta": ruta,
                    "cargos": cargos_por_guia.get(guia, []),  # <- cargos adicionales ya filtrados/últimos
                }

        # ============================================================
        # 4) Parse naviera (COSCO / ONE)
        # ============================================================
        if job.naviera.upper() == "COSCO":
            nav_parser = COSCOFacturacionParser()
        else:
            nav_parser = ONEFacturacionParser()

        nav_rows_iter: Iterable[dict] = nav_parser.parse(fact_path)

        # ============================================================
        # 5) Reconcile
        # ============================================================
        resumen, det_cont, det_cargos, excs = reconcile(
            job.naviera,
            fils_rows=list(iter_fils_dicts()),   # reconcile hoy espera List[dict] (firma actual)
            naviera_rows=list(nav_rows_iter),    # idem
            money_tolerance=tol
        )

        # ============================================================
        # 6) Persist results
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


