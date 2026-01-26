# app/models/result_summary.py

from app.extensions import db


class ResultSummary(db.Model):
    __tablename__ = "result_summary"
    __table_args__ = {"schema": "auditoria"}

    id = db.Column(db.Integer, primary_key=True)

    job_id = db.Column(
        db.Integer,
        db.ForeignKey("auditoria.jobs.id"),
        nullable=False,
        index=True,
    )

    guia = db.Column(db.String(50), nullable=False, index=True)
    estado = db.Column(db.String(30), nullable=False)  # CERRADA / NO_CERRADA / etc.

    total_fils = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    total_naviera = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    diferencia = db.Column(db.Numeric(14, 2), nullable=False, default=0)

    ok = db.Column(db.Boolean, nullable=False, default=False)
    naviera = db.Column(db.String(30), nullable=False)  # ONE / COSCO

    # trazabilidad (opcional pero recomendado)
    fuente_naviera = db.Column(db.String(80))  # hoja origen o identificador