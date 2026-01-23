# tests/test_parsers_fils.py

import pandas as pd
from app.parsers.fils_auditoria import FILSAuditoriaParser

def test_fils_parser_sniff_and_parse(tmp_path):
    # Excel mínimo con columnas típicas
    df = pd.DataFrame([
        {"Guía": "1001", "Contenedor": "MSCU1234567", "Estado": "CERRADA", "Monto Tarifa": 1000},
        {"Guía": "1002", "Contenedor": "MSCU9999999", "Estado": "ABIERTA", "Monto Tarifa": 500},
    ])

    xlsx_path = tmp_path / "ReporteGuiaAuditoria_test.xlsx"
    df.to_excel(xlsx_path, index=False)

    p = FILSAuditoriaParser()
    meta = p.sniff(str(xlsx_path))

    assert "errors" in meta
    assert meta["errors"] == []  # Debe reconocer guía
    assert "mapped_columns" in meta
    assert meta["mapped_columns"]["guia"] is not None

    rows = p.parse(str(xlsx_path))
    assert isinstance(rows, list)
    assert len(rows) == 2
    assert rows[0]["guia"] == "1001"
    assert rows[0]["contenedor"] == "MSCU1234567"