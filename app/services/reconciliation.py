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
    contenedor: str = ""


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
    fils_rows: List[dict] | Any,
    naviera_rows: List[dict] | Any,
    money_tolerance: Decimal
) -> Tuple[List[ReconRow], List[dict], List[dict], List[ReconException]]:

    naviera_up = naviera.upper()

    # -------------------------
    # 1) Indexar FILS por guía (última cerrada) + index por contenedor
    # -------------------------
    fils_by_guia: Dict[str, List[dict]] = {}
    for r in fils_rows:
        g = str(r.get("guia", "")).strip()
        if not g:
            continue
        fils_by_guia.setdefault(g, []).append(r)

    def pick_last_closed(rows: List[dict]) -> Tuple[str, dict]:
        closed = [x for x in rows if str(x.get("estado", "")).upper().strip() == "CERRADA"]
        if closed:
            closed_sorted = sorted(
                closed,
                key=lambda x: x.get("fecha_cierre") or x.get("fecha") or 0,
                reverse=True,
            )
            return "CERRADA", closed_sorted[0]

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

    # index por contenedor usando la fila “picked”
    fils_by_contenedor: Dict[str, str] = {}  # contenedor -> guia
    for g, r in fils_last.items():
        c = str(r.get("contenedor", "") or "").strip().upper()
        if c:
            fils_by_contenedor[c] = g

    # -------------------------
    # 2) Indexar NAVIERA:
    #   - por guía cuando exista
    #   - por contenedor cuando no exista guía
    # -------------------------
    nav_by_guia: Dict[str, List[dict]] = {}
    nav_by_contenedor: Dict[str, List[dict]] = {}

    for r in naviera_rows:
        g = str(r.get("guia", "")).strip()
        c = str(r.get("contenedor", "")).strip().upper()
        if g:
            nav_by_guia.setdefault(g, []).append(r)
        elif c:
            nav_by_contenedor.setdefault(c, []).append(r)

    # -------------------------
    # 3) Universo de guías a evaluar
    #   - todas las de FILS
    #   - más las que aparezcan por guía en naviera
    #   - más las que aparezcan por contenedor en naviera (si ese contenedor liga a guía FILS)
    # -------------------------
    all_guias = set(fils_last.keys()) | set(nav_by_guia.keys())
    for cont, rows in nav_by_contenedor.items():
        g = fils_by_contenedor.get(cont)
        if g:
            all_guias.add(g)

    all_guias = sorted(all_guias)

    resumen: List[ReconRow] = []
    excepciones: List[ReconException] = []
    detalle_cont: List[dict] = []
    detalle_cargos: List[dict] = []

    for guia in all_guias:
        fils_r = fils_last.get(guia)
        cont = (str(fils_r.get("contenedor") or "").strip().upper() if fils_r else "")

        nav_rs = nav_by_guia.get(guia)
        # Si naviera no trae guía, probamos por contenedor
        if not nav_rs and cont:
            nav_rs = nav_by_contenedor.get(cont)

        # totals naviera
        total_nav = sum(parse_money(x.get("total_naviera")) for x in (nav_rs or []))

        # total FILS = monto_tarifa(monto_total) + cargos adicionales (total_naviera de cargos)
        total_fils = Decimal("0")
        if fils_r:
            total_fils = parse_money(fils_r.get("monto_total") or 0)
            # sumar cargos adicionales FILS (ya vienen filtrados en job_runner)
            cargos = fils_r.get("cargos") or []
            total_cargos = sum(parse_money(c.get("total_naviera") or c.get("monto") or 0) for c in cargos)
            total_fils = total_fils + total_cargos

        # casos solo en uno u otro
        if not fils_r and nav_rs:
            # naviera tiene algo que FILS no
            ref_cont = str(nav_rs[0].get("contenedor") or "").strip().upper()
            excepciones.append(ReconException(
                tipo="SOLO_EN_NAVIERA",
                guia=guia,
                contenedor=ref_cont,
                detalle="Existe en facturación naviera pero no en FILS.",
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
                fuente_naviera=(nav_rs[0].get("sheet") or ""),
                contenedor=ref_cont
            ))
            continue

        if fils_r and not nav_rs:
            estado = fils_estado.get(guia, "NO_CERRADA")
            excepciones.append(ReconException(
                tipo="SOLO_EN_FILS",
                guia=guia,
                contenedor=cont,
                detalle="Existe en FILS pero no en facturación naviera (ni por guía ni por contenedor).",
                severidad="ERROR",
                naviera=naviera_up
            ))
            if estado == "NO_CERRADA":
                excepciones.append(ReconException(
                    tipo="NO_CERRADA",
                    guia=guia,
                    contenedor=cont,
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
                contenedor=cont
            ))
            continue

        # ambos existen
        diff = total_fils - total_nav
        ok = abs(diff) <= money_tolerance

        estado = fils_estado.get(guia, "NO_CERRADA")
        if estado == "NO_CERRADA":
            excepciones.append(ReconException(
                tipo="NO_CERRADA",
                guia=guia,
                contenedor=cont,
                detalle="No se encontró guía CERRADA para esta guía en FILS.",
                severidad="WARN",
                naviera=naviera_up
            ))

        if not ok:
            excepciones.append(ReconException(
                tipo="DIFERENCIA",
                guia=guia,
                contenedor=cont,
                detalle=f"Diferencia. FILS={total_fils} vs NAVIERA={total_nav}.",
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
            fuente_naviera=(nav_rs[0].get("sheet") or "") if nav_rs else "",
            contenedor=cont
        ))

        detalle_cont.append({
            "guia": guia,
            "contenedor": cont,
            "ruta": (fils_r.get("ruta") or (nav_rs[0].get("ruta") if nav_rs else "")) if fils_r else "",
            "flete": "0",
            "extras": "0",
            "total": str(total_fils),
            "naviera": naviera_up,
        })

        # detalle cargos FILS (adicionales)
        for c in (fils_r.get("cargos") or []):
            detalle_cargos.append({
                "guia": guia,
                "contenedor": cont,
                "tipo_cargo": c.get("tipo_cargo") or c.get("cargo") or "CARGO_ADICIONAL",
                "monto": str(parse_money(c.get("total_naviera") or c.get("monto") or 0)),
                "origen": "FILS",
                "naviera": naviera_up,
            })

        # detalle cargos NAVIERA si vienen desglosados
        for n in (nav_rs or []):
            cargo = str(n.get("cargo") or "").strip()
            if cargo:
                detalle_cargos.append({
                    "guia": guia,
                    "contenedor": cont,
                    "tipo_cargo": cargo,
                    "monto": str(parse_money(n.get("total_naviera") or 0)),
                    "origen": "NAVIERA",
                    "naviera": naviera_up,
                })

    logger.info(f"Reconciliation done naviera={naviera_up} resumen={len(resumen)} excepciones={len(excepciones)}")
    return resumen, detalle_cont, detalle_cargos, excepciones
