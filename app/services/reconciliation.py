# app/services/reconciliation.py

from dataclasses import dataclass
from typing import Dict, List, Tuple, Any, Iterable, Optional
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


def _row_date_key(r: dict) -> Any:
    """
    Ordena por fecha: intenta fecha_cierre, luego fecha.
    """
    return r.get("fecha_cierre") or r.get("fecha") or 0


def reconcile(
    naviera: str,
    fils_rows: Iterable[dict],
    naviera_rows: Iterable[dict],
    money_tolerance: Decimal
) -> Tuple[List[ReconRow], List[dict], List[dict], List[ReconException]]:
    """
    Streaming reconcile (no requiere listas enormes).
    fils_rows: iterable de dicts normalizados por parser FILS.
    naviera_rows: iterable de dicts normalizados por parser naviera.

    Retorna:
      - resumen_guias (ReconRow)
      - detalle_contenedores (list dict)
      - detalle_cargos (list dict) (MVP: solo cargos FILS si vienen)
      - excepciones (ReconException)
    """
    naviera_up = naviera.upper().strip()

    # ---------------------------------------------------------
    # 1) FILS: mantener por guía:
    #    - mejor CERRADA (más reciente)
    #    - mejor ANY (más reciente)
    # ---------------------------------------------------------
    fils_best_closed: Dict[str, dict] = {}
    fils_best_any: Dict[str, dict] = {}

    fils_seen = 0
    for r in fils_rows:
        fils_seen += 1
        g = str(r.get("guia", "")).strip()
        if not g:
            continue

        # best any
        cur_any = fils_best_any.get(g)
        if cur_any is None or _row_date_key(r) > _row_date_key(cur_any):
            fils_best_any[g] = r

        # best closed
        estado_r = str(r.get("estado", "")).upper().strip()
        if estado_r == "CERRADA":
            cur_closed = fils_best_closed.get(g)
            if cur_closed is None or _row_date_key(r) > _row_date_key(cur_closed):
                fils_best_closed[g] = r

        if fils_seen % 10000 == 0:
            logger.info(f"FILS streaming: filas={fils_seen} guias_any={len(fils_best_any)} guias_closed={len(fils_best_closed)}")

    # resolver FILS final por guía
    fils_last: Dict[str, dict] = {}
    fils_estado: Dict[str, str] = {}

    for g, any_r in fils_best_any.items():
        if g in fils_best_closed:
            fils_last[g] = fils_best_closed[g]
            fils_estado[g] = "CERRADA"
        else:
            fils_last[g] = any_r
            fils_estado[g] = "NO_CERRADA"

    # ---------------------------------------------------------
    # 2) Naviera: sumar por guía (sin guardar todas las filas)
    # ---------------------------------------------------------
    nav_totals: Dict[str, Decimal] = {}
    nav_first_sheet: Dict[str, str] = {}
    nav_first_ruta: Dict[str, str] = {}
    nav_seen = 0

    for r in naviera_rows:
        nav_seen += 1
        g = str(r.get("guia", "")).strip()
        if not g:
            continue

        nav_totals[g] = nav_totals.get(g, Decimal("0")) + parse_money(r.get("total_naviera"))
        if g not in nav_first_sheet:
            nav_first_sheet[g] = str(r.get("sheet") or "").strip()
        if g not in nav_first_ruta:
            nav_first_ruta[g] = str(r.get("ruta") or "").strip()

        if nav_seen % 10000 == 0:
            logger.info(f"NAVIERA streaming: filas={nav_seen} guias={len(nav_totals)}")

    # ---------------------------------------------------------
    # 3) Universo de guías
    # ---------------------------------------------------------
    all_guias = sorted(set(list(fils_last.keys()) + list(nav_totals.keys())))

    resumen: List[ReconRow] = []
    excepciones: List[ReconException] = []
    detalle_cont: List[dict] = []
    detalle_cargos: List[dict] = []

    for guia in all_guias:
        fils_r = fils_last.get(guia)
        total_nav = nav_totals.get(guia)

        # Caso: solo en naviera
        if not fils_r and total_nav is not None:
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
                fuente_naviera=nav_first_sheet.get(guia, "")
            ))
            continue

        # Caso: solo en FILS
        if fils_r and total_nav is None:
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

        # Caso: ambos existen
        total_naviera = total_nav or Decimal("0")

        # Total FILS: si monto_total existe úsalo; si no, flete + extras
        total_fils = parse_money(fils_r.get("monto_total") or 0)
        if total_fils == 0:
            total_fils = parse_money(fils_r.get("monto_flete") or 0) + parse_money(fils_r.get("monto_extras") or 0)

        diff = total_fils - total_naviera
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
                detalle=f"Diferencia detectada. FILS={total_fils} vs NAVIERA={total_naviera}.",
                severidad="ERROR",
                naviera=naviera_up
            ))

        resumen.append(ReconRow(
            guia=guia,
            estado=estado,
            total_fils=total_fils,
            total_naviera=total_naviera,
            diferencia=diff,
            ok=ok,
            naviera=naviera_up,
            fuente_naviera=nav_first_sheet.get(guia, "")
        ))

        # Detalle contenedores (MVP: 1 por guía)
        detalle_cont.append({
            "guia": guia,
            "contenedor": (fils_r.get("contenedor") or ""),
            "ruta": (fils_r.get("ruta") or nav_first_ruta.get(guia, "")),
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

    logger.info(
        f"Reconciliation done naviera={naviera_up} "
        f"resumen={len(resumen)} excepciones={len(excepciones)} "
        f"fils_filas={fils_seen} nav_filas={nav_seen}"
    )
    return resumen, detalle_cont, detalle_cargos, excepciones
