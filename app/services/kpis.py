# app/services/kpis.py

from decimal import Decimal
from typing import List, Dict

from app.utils.money import parse_money

def compute_kpis(naviera: str, resumen_rows: List[dict]) -> Dict:
    """
    resumen_rows: lista de dict con keys:
      guia, ok, estado, total_fils, total_naviera, diferencia
    """
    total_guias = len(resumen_rows)
    guias_ok = sum(1 for r in resumen_rows if bool(r.get("ok")))
    guias_diferencia = sum(1 for r in resumen_rows if not bool(r.get("ok")))
    guias_no_cerrada = sum(1 for r in resumen_rows if str(r.get("estado","")).upper() == "NO_CERRADA")
    guias_solo_en_fils = sum(1 for r in resumen_rows if str(r.get("estado","")).upper() == "SIN_NAVIERA" or str(r.get("estado","")).upper() == "SIN_FACTURACION")
    guias_solo_en_naviera = sum(1 for r in resumen_rows if str(r.get("estado","")).upper() == "SIN_FILS")

    total_fils = sum(parse_money(r.get("total_fils")) for r in resumen_rows)
    total_naviera = sum(parse_money(r.get("total_naviera")) for r in resumen_rows)
    diferencia_global = total_fils - total_naviera

    return {
        "naviera": naviera,
        "total_guias": total_guias,
        "guias_ok": guias_ok,
        "guias_diferencia": guias_diferencia,
        "guias_no_cerrada": guias_no_cerrada,
        "guias_solo_en_fils": guias_solo_en_fils,
        "guias_solo_en_naviera": guias_solo_en_naviera,
        "total_fils": str(total_fils),
        "total_naviera": str(total_naviera),
        "diferencia_global": str(diferencia_global),
        "pct_ok": (guias_ok / total_guias * 100) if total_guias else 0,
    }