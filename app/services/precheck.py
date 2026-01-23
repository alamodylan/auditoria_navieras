# app/services/precheck.py

from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional

from app.utils.logging import get_logger
from app.parsers.fils_auditoria import FILSAuditoriaParser
from app.parsers.cosco_facturacion import COSCOFacturacionParser
from app.parsers.one_facturacion import ONEFacturacionParser

logger = get_logger("precheck")


@dataclass
class PrecheckIssue:
    level: str  # "ERROR" | "WARN"
    message: str
    context: Optional[dict] = None


@dataclass
class PrecheckReport:
    ok: bool
    naviera: str
    issues: List[PrecheckIssue]
    meta: Dict[str, Any]


def run_precheck(naviera: str, fils_path: str, facturacion_path: str) -> PrecheckReport:
    issues: List[PrecheckIssue] = []
    meta: Dict[str, Any] = {"naviera": naviera}

    # FILS siempre
    fils_parser = FILSAuditoriaParser()
    fils_meta = fils_parser.sniff(fils_path)
    meta["fils"] = fils_meta

    for msg in fils_meta.get("errors", []):
        issues.append(PrecheckIssue("ERROR", msg))
    for msg in fils_meta.get("warnings", []):
        issues.append(PrecheckIssue("WARN", msg))

    # Naviera
    naviera_up = naviera.upper().strip()
    if naviera_up == "COSCO":
        parser = COSCOFacturacionParser()
    elif naviera_up == "ONE":
        parser = ONEFacturacionParser()
    else:
        issues.append(PrecheckIssue("ERROR", f"Naviera no soportada: {naviera}"))
        return PrecheckReport(ok=False, naviera=naviera_up, issues=issues, meta=meta)

    nav_meta = parser.sniff(facturacion_path)
    meta["facturacion"] = nav_meta

    for msg in nav_meta.get("errors", []):
        issues.append(PrecheckIssue("ERROR", msg))
    for msg in nav_meta.get("warnings", []):
        issues.append(PrecheckIssue("WARN", msg))

    ok = not any(i.level == "ERROR" for i in issues)

    report = PrecheckReport(ok=ok, naviera=naviera_up, issues=issues, meta=meta)
    logger.info(f"Precheck naviera={naviera_up} ok={ok} issues={len(issues)}")
    return report


def report_to_dict(report: PrecheckReport) -> dict:
    d = asdict(report)
    d["issues"] = [asdict(i) for i in report.issues]
    return d