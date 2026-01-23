# app/services/reconciliation.py

from dataclasses import dataclass
from typing import Dict, List, Tuple, Any
from decimal import Decimal

from app.utils.money import parse_money
from app.utils.logging import get_logger

logger = get_logger("reconciliation")


@dataclass
class ReconRow:
    guia: str
    estado: str
    total_fils: Decimal
    total_naviera: Decimal
    diferencia: Decimal
    ok: bool
    naviera: str
    fuente_naviera: str = ""


@dataclass
class ReconException:
    tipo: str
    guia: str = ""
    contenedor: str = ""
    detalle: str = ""
    severidad: str = "WARN"
    naviera: str = ""


def reconcile(
    naviera: str,
    fils_rows: List[dict],
    naviera_rows: List[dict],
    money_tolerance: Decimal
) -> Tuple[List[ReconRow], List[dict], List[dict], List[ReconException]]:
    """
    fils_rows: lista de dicts normalizados por parser FILS
      campos esperados:
        guia, contenedor, estado, fecha_cierre (optional),
        monto_flete (optional), monto_total (optional), monto_extra (optional)
    naviera_rows: lista dicts normalizados
      campos esperados:
        guia, contenedor (optional), total_naviera, sheet (optional), ruta (optional)
    Retorna:
      - resumen_guias (ReconRow)
      - detalle_contenedores (list dict)
      - detalle_cargos (list dict) (MVP: solo cargos FILS si vienen)
      - excepciones (ReconException)
    """

    naviera_up = naviera.upper()

    # 1) Indexar FILS por guia: elegir última CERRADA, si no, NO_CERRADA
    fils_by_guia: Dict[str, List[dict]] = {}
    for r in fils_rows:
        g = str(r.get("guia", "")).strip()
        if not g:
            continue
        fils_by_guia.setdefault(g, []).append(r)

    def pick_last_closed(rows: List[dict]) -> Tuple[str, dict]:
        # Preferir estado CERRADA; si hay fecha_cierre, usarla DESC
        closed = [x for x in rows if str(x.get("estado", "")).upper().strip() == "CERRADA"]
        if closed:
            closed_sorted = sorted(
                closed,
                key=lambda x: x.get("fecha_cierre") or x.get("fecha") or 0,
                reverse=True,
            )
            return "CERRADA", closed_sorted[0]
        # si no hay cerrada
        # pick "más reciente" cualquiera para tener datos
        any_sorted = sorted(
            rows,
            key=lambda x: x.get("fecha_cierre") or x.get("fecha") or 0,
            reverse=True,
        )
        return "NO_CERRADA", any_sorted[0]

    fils_last: Dict[str, dict] = {}
    fils_estado: Dict[str, str] = {}
    for g, rows in fils_by_guia.items():
        estado, picked = pick_last_closed(rows)
        fils_last[g] = picked
        fils_estado[g] = estado

    # 2) Indexar naviera por guia (puede haber varias filas -> sumar)
    nav_by_guia: Dict[str, List[dict]] = {}
    for r in naviera_rows:
        g = str(r.get("guia", "")).strip()
        if not g:
            continue
        nav_by_guia.setdefault(g, []).append(r)

    # 3) Construir universo de guías
    all_guias = sorted(set(list(fils_last.keys()) + list(nav_by_guia.keys())))

    resumen: List[ReconRow] = []
    excepciones: List[ReconException] = []
    detalle_cont: List[dict] = []
    detalle_cargos: List[dict] = []

    for guia in all_guias:
        fils_r = fils_last.get(guia)
        nav_rs = nav_by_guia.get(guia)

        if not fils_r and nav_rs:
            total_nav = sum(parse_money(x.get("total_naviera")) for x in nav_rs)
            excepciones.append(ReconException(
                tipo="SOLO_EN_NAVIERA",
                guia=guia,
                detalle="Guía existe en facturación naviera pero no en FILS.",
                severidad="ERROR",
                naviera=naviera_up
            ))
            resumen.append(ReconRow(
                guia=guia,
                estado="SIN_FILS",
                total_fils=Decimal("0"),
                total_naviera=total_nav,
                diferencia=total_nav,
                ok=False,
                naviera=naviera_up,
                fuente_naviera=(nav_rs[0].get("sheet") or "")
            ))
            continue

        if fils_r and not nav_rs:
            total_fils = parse_money(fils_r.get("monto_total") or fils_r.get("monto_flete") or 0)
            estado = fils_estado.get(guia, "NO_CERRADA")
            excepciones.append(ReconException(
                tipo="SOLO_EN_FILS",
                guia=guia,
                detalle="Guía existe en FILS pero no en facturación naviera.",
                severidad="ERROR",
                naviera=naviera_up
            ))
            if estado == "NO_CERRADA":
                excepciones.append(ReconException(
                    tipo="NO_CERRADA",
                    guia=guia,
                    detalle="No se encontró guía CERRADA para esta guía en FILS.",
                    severidad="WARN",
                    naviera=naviera_up
                ))
            resumen.append(ReconRow(
                guia=guia,
                estado=estado,
                total_fils=total_fils,
                total_naviera=Decimal("0"),
                diferencia=total_fils,
                ok=False,
                naviera=naviera_up,
            ))
            continue

        # ambos existen
        total_nav = sum(parse_money(x.get("total_naviera")) for x in (nav_rs or []))

        # Total FILS: si parser da monto_total úsalo; si no, flete + extras
        total_fils = parse_money(fils_r.get("monto_total") or 0)
        if total_fils == 0:
            total_fils = parse_money(fils_r.get("monto_flete") or 0) + parse_money(fils_r.get("monto_extras") or 0)

        diff = total_fils - total_nav
        ok = abs(diff) <= money_tolerance

        estado = fils_estado.get(guia, "NO_CERRADA")
        if estado == "NO_CERRADA":
            excepciones.append(ReconException(
                tipo="NO_CERRADA",
                guia=guia,
                detalle="No se encontró guía CERRADA para esta guía en FILS.",
                severidad="WARN",
                naviera=naviera_up
            ))

        if not ok:
            excepciones.append(ReconException(
                tipo="DIFERENCIA",
                guia=guia,
                detalle=f"Diferencia detectada. FILS={total_fils} vs NAVIERA={total_nav}.",
                severidad="ERROR",
                naviera=naviera_up
            ))

        resumen.append(ReconRow(
            guia=guia,
            estado=estado,
            total_fils=total_fils,
            total_naviera=total_nav,
            diferencia=diff,
            ok=ok,
            naviera=naviera_up,
            fuente_naviera=(nav_rs[0].get("sheet") or "") if nav_rs else ""
        ))

        # Detalle contenedores (MVP: uno por guía con contenedor principal si existe)
        detalle_cont.append({
            "guia": guia,
            "contenedor": (fils_r.get("contenedor") or ""),
            "ruta": (fils_r.get("ruta") or nav_rs[0].get("ruta") if nav_rs else ""),
            "flete": str(parse_money(fils_r.get("monto_flete") or 0)),
            "extras": str(parse_money(fils_r.get("monto_extras") or 0)),
            "total": str(total_fils),
            "naviera": naviera_up,
        })

        # Detalle cargos (si FILS trae lista)
        cargos = fils_r.get("cargos") or []
        for c in cargos:
            detalle_cargos.append({
                "guia": guia,
                "contenedor": c.get("contenedor") or fils_r.get("contenedor") or "",
                "tipo_cargo": c.get("tipo_cargo") or "CARGO",
                "monto": str(parse_money(c.get("monto") or 0)),
                "origen": "FILS",
                "naviera": naviera_up,
            })

    logger.info(f"Reconciliation done naviera={naviera_up} resumen={len(resumen)} excepciones={len(excepciones)}")
    return resumen, detalle_cont, detalle_cargos, excepciones