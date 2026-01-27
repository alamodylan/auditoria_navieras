# app/services/reconciliation.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Iterable, Optional
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


def _norm_contenedor(value: str) -> str:
    return str(value or "").strip().upper().replace("-", "").replace(" ", "")


def _cargo_key_from_fils(c: dict) -> str:
    """
    FILS cargos (desde fils_auditoria.py) vienen así:
      {cargo_id, cargo, moneda, monto}
    """
    cargo_id = str(c.get("cargo_id") or "").strip()
    cargo = str(c.get("cargo") or "").strip()
    if cargo_id:
        return f"ID:{cargo_id}"
    return cargo.upper()


def _cargo_key_from_naviera(r: dict) -> str:
    """
    Facturación naviera puede venir con varios nombres.
    Intentamos ser tolerantes:
      - cargo_id / charge_id
      - cargo / tipo_cargo / concepto / descripcion
    """
    cargo_id = str(r.get("cargo_id") or r.get("charge_id") or "").strip()
    if cargo_id:
        return f"ID:{cargo_id}"
    name = (
        r.get("cargo")
        or r.get("tipo_cargo")
        or r.get("concepto")
        or r.get("descripcion")
        or r.get("description")
        or "CARGO"
    )
    return str(name).strip().upper()


def _sum_nav_total(rows: List[dict]) -> Decimal:
    """
    Total naviera por guía o por contenedor.
    Soporta diferentes llaves del parser:
      total_naviera / total / monto / amount
    """
    total = Decimal("0")
    for r in rows:
        v = (
            r.get("total_naviera")
            if r.get("total_naviera") is not None
            else r.get("total")
            if r.get("total") is not None
            else r.get("monto")
            if r.get("monto") is not None
            else r.get("amount")
        )
        total += parse_money(v)
    return total


def _build_nav_cargos(rows: List[dict]) -> Dict[str, Decimal]:
    """
    Construye mapa cargo_key -> monto_sum para NAVIERA.
    Si el archivo de ONE trae cargos adicionales como filas separadas, esto lo captura.
    Si trae solo un "Monto" total sin desglose, igual habrá 1 cargo genérico.
    """
    cargos: Dict[str, Decimal] = {}
    for r in rows:
        key = _cargo_key_from_naviera(r)

        v = (
            r.get("monto")
            if r.get("monto") is not None
            else r.get("amount")
            if r.get("amount") is not None
            else r.get("total_naviera")
            if r.get("total_naviera") is not None
            else r.get("total")
        )
        cargos[key] = cargos.get(key, Decimal("0")) + parse_money(v)
    return cargos


def _build_fils_cargos(fils_r: dict) -> Dict[str, Decimal]:
    """
    Mapa cargo_key -> monto.
    En FILS ya filtramos por 'última actualización' y omitimos 'Eliminar',
    así que aquí solo sumamos/agrupamos por cargo.
    """
    cargos_list = fils_r.get("cargos") or []
    cargos: Dict[str, Decimal] = {}
    for c in cargos_list:
        key = _cargo_key_from_fils(c)
        cargos[key] = cargos.get(key, Decimal("0")) + parse_money(c.get("monto"))
    return cargos


def reconcile(
    naviera: str,
    fils_rows: Iterable[dict],
    naviera_rows: Iterable[dict],
    money_tolerance: Decimal,
) -> Tuple[List[ReconRow], List[dict], List[dict], List[ReconException]]:
    """
    Reglas clave:
    - FILS puede venir con múltiples filas por guía (eventos). Se escoge la última CERRADA,
      si no hay CERRADA, se usa la más reciente y estado=NO_CERRADA.
    - ONE a veces trae guía y a veces no:
        * si trae guía -> match por guía
        * si NO trae guía -> match por contenedor (usando hoja Contenedor de FILS para ligar guía<->contenedor)
    - Cargos adicionales:
        * FILS los trae desde hoja Cargos Adicionales (último evento por cargo; si termina en Eliminar, se descarta)
        * NAVIERA puede traer desglose o puede traer solo un monto total. Se intenta comparar cargos cuando existan.
    - Totales:
        total_fils = monto_total (Monto Tarifa) + SUM(cargos_naviera_en_fils)
        total_nav  = SUM(montos naviera rows del match)
    """

    naviera_up = naviera.upper()

    # -----------------------------
    # 1) Indexar FILS por guía
    # -----------------------------
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

    # -----------------------------
    # 2) Indexar FILS por contenedor (fallback para ONE sin guía)
    # -----------------------------
    fils_by_cont: Dict[str, List[dict]] = {}
    for g, r in fils_last.items():
        cont = _norm_contenedor(r.get("contenedor", ""))
        if cont:
            fils_by_cont.setdefault(cont, []).append(r)

    def pick_best_by_cont(rows: List[dict]) -> dict:
        return sorted(
            rows,
            key=lambda x: x.get("fecha_cierre") or x.get("fecha") or 0,
            reverse=True,
        )[0]

    # -----------------------------
    # 3) Indexar NAVIERA
    # -----------------------------
    nav_by_guia: Dict[str, List[dict]] = {}
    nav_no_guia: List[dict] = []

    for r in naviera_rows:
        g = str(r.get("guia", "")).strip()
        cont = _norm_contenedor(r.get("contenedor", ""))
        if g:
            nav_by_guia.setdefault(g, []).append(r)
        else:
            if cont:
                r["_contenedor_norm"] = cont
                nav_no_guia.append(r)

    # -----------------------------
    # 4) Universo inicial por GUÍA (match directo)
    # -----------------------------
    all_guias = sorted(set(list(fils_last.keys()) + list(nav_by_guia.keys())))

    resumen: List[ReconRow] = []
    excepciones: List[ReconException] = []
    detalle_cont: List[dict] = []
    detalle_cargos: List[dict] = []

    # helper para evitar buscar linealmente luego
    resumen_by_guia: Dict[str, ReconRow] = {}

    for guia in all_guias:
        fils_r = fils_last.get(guia)
        nav_rs = nav_by_guia.get(guia)

        if not fils_r and nav_rs:
            total_nav = _sum_nav_total(nav_rs)
            excepciones.append(
                ReconException(
                    tipo="SOLO_EN_NAVIERA",
                    guia=guia,
                    detalle="Guía existe en facturación naviera pero no en FILS.",
                    severidad="ERROR",
                    naviera=naviera_up,
                )
            )
            row = ReconRow(
                guia=guia,
                estado="SIN_FILS",
                total_fils=Decimal("0"),
                total_naviera=total_nav,
                diferencia=total_nav,
                ok=False,
                naviera=naviera_up,
                fuente_naviera=(nav_rs[0].get("sheet") or ""),
            )
            resumen.append(row)
            resumen_by_guia[guia] = row
            continue

        if fils_r and not nav_rs:
            base_fils = parse_money(fils_r.get("monto_total") or fils_r.get("monto_flete") or 0)
            cargos_fils = _build_fils_cargos(fils_r)
            total_fils = base_fils + sum(cargos_fils.values(), Decimal("0"))

            estado = fils_estado.get(guia, "NO_CERRADA")
            excepciones.append(
                ReconException(
                    tipo="SOLO_EN_FILS",
                    guia=guia,
                    detalle="Guía existe en FILS pero no en facturación naviera.",
                    severidad="ERROR",
                    naviera=naviera_up,
                )
            )
            if estado == "NO_CERRADA":
                excepciones.append(
                    ReconException(
                        tipo="NO_CERRADA",
                        guia=guia,
                        detalle="No se encontró guía CERRADA para esta guía en FILS.",
                        severidad="WARN",
                        naviera=naviera_up,
                    )
                )

            row = ReconRow(
                guia=guia,
                estado=estado,
                total_fils=total_fils,
                total_naviera=Decimal("0"),
                diferencia=total_fils,
                ok=False,
                naviera=naviera_up,
            )
            resumen.append(row)
            resumen_by_guia[guia] = row

            # detalle contenedor (aunque no haya naviera)
            detalle_cont.append(
                {
                    "guia": guia,
                    "contenedor": fils_r.get("contenedor") or "",
                    "ruta": fils_r.get("ruta") or "",
                    "flete": str(parse_money(fils_r.get("monto_flete") or 0)),
                    "extras": str(parse_money(fils_r.get("monto_extras") or 0)),
                    "total": str(total_fils),
                    "naviera": naviera_up,
                }
            )
            # cargos FILS
            for k, amt in cargos_fils.items():
                detalle_cargos.append(
                    {
                        "guia": guia,
                        "contenedor": fils_r.get("contenedor") or "",
                        "tipo_cargo": k,
                        "monto": str(amt),
                        "origen": "FILS",
                        "naviera": naviera_up,
                    }
                )
            continue

        # ambos existen por guía
        assert fils_r is not None and nav_rs is not None

        base_fils = parse_money(fils_r.get("monto_total") or 0)
        if base_fils == 0:
            base_fils = parse_money(fils_r.get("monto_flete") or 0) + parse_money(fils_r.get("monto_extras") or 0)

        cargos_fils = _build_fils_cargos(fils_r)
        total_fils = base_fils + sum(cargos_fils.values(), Decimal("0"))

        total_nav = _sum_nav_total(nav_rs)
        diff = total_fils - total_nav
        ok = abs(diff) <= money_tolerance

        estado = fils_estado.get(guia, "NO_CERRADA")
        if estado == "NO_CERRADA":
            excepciones.append(
                ReconException(
                    tipo="NO_CERRADA",
                    guia=guia,
                    detalle="No se encontró guía CERRADA para esta guía en FILS.",
                    severidad="WARN",
                    naviera=naviera_up,
                )
            )

        if not ok:
            excepciones.append(
                ReconException(
                    tipo="DIFERENCIA",
                    guia=guia,
                    detalle=f"Diferencia detectada. FILS={total_fils} vs NAVIERA={total_nav}.",
                    severidad="ERROR",
                    naviera=naviera_up,
                )
            )

        # Comparación de cargos (cuando sea posible)
        nav_cargos = _build_nav_cargos(nav_rs)
        if cargos_fils:
            # Si FILS tiene cargos, intentamos comparar el desglose
            union_keys = set(cargos_fils.keys()) | set(nav_cargos.keys())
            for ck in sorted(union_keys):
                a = cargos_fils.get(ck, Decimal("0"))
                b = nav_cargos.get(ck, Decimal("0"))
                d = a - b
                if abs(d) > money_tolerance:
                    excepciones.append(
                        ReconException(
                            tipo="CARGO_DIFERENCIA",
                            guia=guia,
                            contenedor=_norm_contenedor(fils_r.get("contenedor", "")),
                            detalle=f"Cargo '{ck}' difiere. FILS={a} vs NAVIERA={b}.",
                            severidad="ERROR",
                            naviera=naviera_up,
                        )
                    )
                if a == 0 and b != 0:
                    excepciones.append(
                        ReconException(
                            tipo="CARGO_SOLO_NAVIERA",
                            guia=guia,
                            contenedor=_norm_contenedor(fils_r.get("contenedor", "")),
                            detalle=f"Cargo '{ck}' existe en NAVIERA pero no en FILS.",
                            severidad="WARN",
                            naviera=naviera_up,
                        )
                    )
                if a != 0 and b == 0:
                    excepciones.append(
                        ReconException(
                            tipo="CARGO_SOLO_FILS",
                            guia=guia,
                            contenedor=_norm_contenedor(fils_r.get("contenedor", "")),
                            detalle=f"Cargo '{ck}' existe en FILS pero no en NAVIERA.",
                            severidad="WARN",
                            naviera=naviera_up,
                        )
                    )

        row = ReconRow(
            guia=guia,
            estado=estado,
            total_fils=total_fils,
            total_naviera=total_nav,
            diferencia=diff,
            ok=ok,
            naviera=naviera_up,
            fuente_naviera=(nav_rs[0].get("sheet") or ""),
        )
        resumen.append(row)
        resumen_by_guia[guia] = row

        detalle_cont.append(
            {
                "guia": guia,
                "contenedor": (fils_r.get("contenedor") or ""),
                "ruta": (fils_r.get("ruta") or (nav_rs[0].get("ruta") if nav_rs else "")),
                "flete": str(parse_money(fils_r.get("monto_flete") or 0)),
                "extras": str(parse_money(fils_r.get("monto_extras") or 0)),
                "total": str(total_fils),
                "naviera": naviera_up,
            }
        )

        # cargos detalle (FILS)
        for ck, amt in cargos_fils.items():
            detalle_cargos.append(
                {
                    "guia": guia,
                    "contenedor": fils_r.get("contenedor") or "",
                    "tipo_cargo": ck,
                    "monto": str(amt),
                    "origen": "FILS",
                    "naviera": naviera_up,
                }
            )

        # cargos detalle (NAVIERA) - útil para export/auditoría
        for ck, amt in _build_nav_cargos(nav_rs).items():
            detalle_cargos.append(
                {
                    "guia": guia,
                    "contenedor": fils_r.get("contenedor") or "",
                    "tipo_cargo": ck,
                    "monto": str(amt),
                    "origen": "NAVIERA",
                    "naviera": naviera_up,
                }
            )

    # -----------------------------
    # 5) Segundo pase: NAVIERA sin guía -> match por contenedor
    # -----------------------------
    nav_by_cont: Dict[str, List[dict]] = {}
    for r in nav_no_guia:
        cont = r.get("_contenedor_norm") or ""
        if cont:
            nav_by_cont.setdefault(cont, []).append(r)

    for cont, nav_rs in nav_by_cont.items():
        fils_candidates = fils_by_cont.get(cont)

        if not fils_candidates:
            total_nav = _sum_nav_total(nav_rs)
            excepciones.append(
                ReconException(
                    tipo="SOLO_EN_NAVIERA",
                    guia="",
                    contenedor=cont,
                    detalle="En ONE no venía guía; se intentó match por contenedor y no existe en FILS.",
                    severidad="ERROR",
                    naviera=naviera_up,
                )
            )
            resumen.append(
                ReconRow(
                    guia=f"(SIN_GUIA){cont}",
                    estado="SIN_FILS",
                    total_fils=Decimal("0"),
                    total_naviera=total_nav,
                    diferencia=total_nav,
                    ok=False,
                    naviera=naviera_up,
                    fuente_naviera=(nav_rs[0].get("sheet") or ""),
                )
            )
            continue

        fils_r = pick_best_by_cont(fils_candidates)
        guia = str(fils_r.get("guia", "")).strip()
        estado = fils_estado.get(guia, "NO_CERRADA")

        base_fils = parse_money(fils_r.get("monto_total") or 0)
        if base_fils == 0:
            base_fils = parse_money(fils_r.get("monto_flete") or 0) + parse_money(fils_r.get("monto_extras") or 0)

        cargos_fils = _build_fils_cargos(fils_r)
        total_fils = base_fils + sum(cargos_fils.values(), Decimal("0"))

        total_nav = _sum_nav_total(nav_rs)

        # Si la guía ya estaba por match directo, acumulamos naviera aquí (porque son filas extra sin guía)
        existing = resumen_by_guia.get(guia)
        if existing:
            existing.total_naviera = existing.total_naviera + total_nav
            existing.diferencia = existing.total_fils - existing.total_naviera
            existing.ok = abs(existing.diferencia) <= money_tolerance
            existing.fuente_naviera = existing.fuente_naviera or (nav_rs[0].get("sheet") or "")
            diff = existing.diferencia
            ok = existing.ok
        else:
            diff = total_fils - total_nav
            ok = abs(diff) <= money_tolerance
            row = ReconRow(
                guia=guia,
                estado=estado,
                total_fils=total_fils,
                total_naviera=total_nav,
                diferencia=diff,
                ok=ok,
                naviera=naviera_up,
                fuente_naviera=(nav_rs[0].get("sheet") or ""),
            )
            resumen.append(row)
            resumen_by_guia[guia] = row

        if estado == "NO_CERRADA":
            excepciones.append(
                ReconException(
                    tipo="NO_CERRADA",
                    guia=guia,
                    contenedor=cont,
                    detalle="Match por contenedor, pero la guía en FILS no está CERRADA.",
                    severidad="WARN",
                    naviera=naviera_up,
                )
            )

        if not ok:
            excepciones.append(
                ReconException(
                    tipo="DIFERENCIA",
                    guia=guia,
                    contenedor=cont,
                    detalle=f"(Match por contenedor) Diferencia: FILS={total_fils} vs NAVIERA={total_nav}.",
                    severidad="ERROR",
                    naviera=naviera_up,
                )
            )

        # Detalle contenedor (registramos el contenedor real del match)
        detalle_cont.append(
            {
                "guia": guia,
                "contenedor": cont,
                "ruta": (fils_r.get("ruta") or (nav_rs[0].get("ruta") if nav_rs else "")),
                "flete": str(parse_money(fils_r.get("monto_flete") or 0)),
                "extras": str(parse_money(fils_r.get("monto_extras") or 0)),
                "total": str(total_fils),
                "naviera": naviera_up,
            }
        )

        # Detalle cargos NAVIERA para estas filas sin guía
        for ck, amt in _build_nav_cargos(nav_rs).items():
            detalle_cargos.append(
                {
                    "guia": guia,
                    "contenedor": cont,
                    "tipo_cargo": ck,
                    "monto": str(amt),
                    "origen": "NAVIERA",
                    "naviera": naviera_up,
                }
            )

    logger.info(
        f"Reconciliation done naviera={naviera_up} resumen={len(resumen)} excepciones={len(excepciones)}"
    )
    return resumen, detalle_cont, detalle_cargos, excepciones
