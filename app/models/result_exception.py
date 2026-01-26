# app/models/result_exception.py

from app.extensions import db


class ResultException(db.Model):
    __tablename__ = "result_exception"
    __table_args__ = {"schema": "auditoria"}

    id = db.Column(db.Integer, primary_key=True)

    job_id = db.Column(
        db.Integer,
        db.ForeignKey("auditoria.jobs.id"),
        nullable=False,
        index=True,
    )

    tipo = db.Column(
        db.String(80),
        nullable=False
    )  # NO_CERRADA / DIFERENCIA / SOLO_EN_FILS / SOLO_EN_NAVIERA

    guia = db.Column(db.String(50), index=True)
    contenedor = db.Column(db.String(20), index=True)

    detalle = db.Column(db.Text, nullable=False)
    severidad = db.Column(
        db.String(20),
        nullable=False,
        default="WARN"
    )  # WARN / ERROR

    naviera = db.Column(db.String(30), nullable=False)  # ONE / COSCO
