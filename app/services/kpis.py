# app/services/kpis.py

from decimal import Decimal
from typing import Iterable, Dict, Any

from app.utils.money import parse_money


def compute_kpis(naviera: str, resumen_rows: Iterable[dict]) -> Dict[str, Any]:
    """
    resumen_rows: iterable de dict con keys:
      guia, ok, estado, total_fils, total_naviera, diferencia

    NOTA:
    - En reconciliation.py los estados que realmente aparecen son:
        * "SIN_FILS"
        * "CERRADA" / "NO_CERRADA"
      (y "SIN_NAVIERA" no existe hoy como estado explícito).
    - Por eso "SOLO_EN_FILS" lo inferimos por total_naviera==0 y total_fils>0,
      y "SOLO_EN_NAVIERA" por estado=="SIN_FILS".
    """

    total_guias = 0
    guias_ok = 0
    guias_no_ok = 0

    guias_no_cerrada = 0
    guias_solo_en_fils = 0
    guias_solo_en_naviera = 0
    guias_con_diferencia = 0  # ambos existen pero no cuadra

    total_fils = Decimal("0")
    total_naviera = Decimal("0")

    for r in resumen_rows:
        total_guias += 1

        ok = bool(r.get("ok"))
        estado = str(r.get("estado", "")).upper().strip()

        tf = parse_money(r.get("total_fils"))
        tn = parse_money(r.get("total_naviera"))

        total_fils += tf
        total_naviera += tn

        if ok:
            guias_ok += 1
        else:
            guias_no_ok += 1

        if estado == "NO_CERRADA":
            guias_no_cerrada += 1

        # SOLO EN NAVIERA: reconciliation lo marca con estado SIN_FILS
        if estado == "SIN_FILS":
            guias_solo_en_naviera += 1
            # no lo cuentes como "diferencia" operativa (es missing)
            continue

        # SOLO EN FILS: inferencia consistente con tu reconcile (tn==0 y tf>0)
        if tn == 0 and tf > 0:
            guias_solo_en_fils += 1
            continue

        # DIFERENCIA: ambos existen (tf>0 y tn>0) pero ok==False
        if (tf > 0 and tn > 0) and (not ok):
            guias_con_diferencia += 1

    diferencia_global = total_fils - total_naviera
    pct_ok = (guias_ok / total_guias * 100) if total_guias else 0

    return {
        "naviera": naviera,
        "total_guias": total_guias,
        "guias_ok": guias_ok,
        "guias_no_ok": guias_no_ok,

        "guias_con_diferencia": guias_con_diferencia,
        "guias_no_cerrada": guias_no_cerrada,
        "guias_solo_en_fils": guias_solo_en_fils,
        "guias_solo_en_naviera": guias_solo_en_naviera,

        "total_fils": str(total_fils),
        "total_naviera": str(total_naviera),
        "diferencia_global": str(diferencia_global),

        "pct_ok": pct_ok,
    }