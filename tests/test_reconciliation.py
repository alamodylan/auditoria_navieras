# tests/test_reconciliation.py

from decimal import Decimal
from app.services.reconciliation import reconcile

def test_reconciliation_last_closed_and_exceptions():
    naviera = "COSCO"
    tol = Decimal("1.00")

    # FILS: misma guía con 2 filas, una NO cerrada y una CERRADA (elige CERRADA)
    fils_rows = [
        {"guia": "3001", "estado": "ABIERTA", "monto_total": 900, "fecha_cierre": None, "contenedor": "AAA"},
        {"guia": "3001", "estado": "CERRADA", "monto_total": 1000, "fecha_cierre": "2026-01-01", "contenedor": "AAA"},
        {"guia": "3002", "estado": "ABIERTA", "monto_total": 500, "fecha_cierre": None, "contenedor": "BBB"},
    ]

    # Naviera
    nav_rows = [
        {"guia": "3001", "total_naviera": 1000, "sheet": "X"},
        {"guia": "3999", "total_naviera": 777, "sheet": "Y"},
    ]

    resumen, det_cont, det_cargos, excs = reconcile(naviera, fils_rows, nav_rows, tol)

    # Debe incluir 3001,3002,3999
    guias = {r.guia for r in resumen}
    assert guias == {"3001", "3002", "3999"}

    # 3001 debe salir OK (1000 vs 1000)
    r3001 = [r for r in resumen if r.guia == "3001"][0]
    assert r3001.estado == "CERRADA"
    assert r3001.ok is True

    # 3002 solo en FILS
    r3002 = [r for r in resumen if r.guia == "3002"][0]
    assert r3002.total_naviera == Decimal("0")
    assert r3002.ok is False

    # 3999 solo en NAVIERA
    r3999 = [r for r in resumen if r.guia == "3999"][0]
    assert r3999.total_fils == Decimal("0")
    assert r3999.ok is False

    tipos = [e.tipo for e in excs]
    assert "SOLO_EN_FILS" in tipos
    assert "SOLO_EN_NAVIERA" in tipos
    assert "NO_CERRADA" in tipos  # por 3002