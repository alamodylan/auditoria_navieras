# tests/test_parsers_cosco.py

import pandas as pd
from app.parsers.cosco_facturacion import COSCOFacturacionParser

def test_cosco_parser_multisheet(tmp_path):
    df1 = pd.DataFrame([
        {"Documento": "2001", "Total": 1000, "Contenedor": "MSCU1234567"},
        {"Documento": "2002", "Total": 1500, "Contenedor": "MSCU9999999"},
    ])
    df2 = pd.DataFrame([
        {"Documento": "2001", "Total": 200, "Contenedor": "MSCU1234567"},  # extra en otra hoja
    ])

    xlsx_path = tmp_path / "COSCO_test.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as w:
        df1.to_excel(w, sheet_name="HOJA1", index=False)
        df2.to_excel(w, sheet_name="HOJA2", index=False)

    p = COSCOFacturacionParser()
    meta = p.sniff(str(xlsx_path))
    assert meta["errors"] == []
    assert "sheets" in meta and len(meta["sheets"]) >= 2

    rows = p.parse(str(xlsx_path))
    assert len(rows) == 3
    assert any(r["guia"] == "2001" for r in rows)
    # Suma de 2001 debe poder hacerse luego en conciliación, aquí solo aseguramos que vienen las filas
    totals_2001 = [r["total_naviera"] for r in rows if r["guia"] == "2001"]
    assert len(totals_2001) == 2