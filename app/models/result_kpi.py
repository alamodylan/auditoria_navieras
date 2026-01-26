# app/models/result_kpi.py

from app.extensions import db


class ResultKPI(db.Model):
    __tablename__ = "result_kpi"
    __table_args__ = {"schema": "auditoria"}

    id = db.Column(db.Integer, primary_key=True)

    job_id = db.Column(
        db.Integer,
        db.ForeignKey("auditoria.jobs.id"),
        nullable=False,
        index=True,
    )

    total_guias = db.Column(db.Integer, nullable=False, default=0)
    guias_ok = db.Column(db.Integer, nullable=False, default=0)
    guias_diferencia = db.Column(db.Integer, nullable=False, default=0)
    guias_no_cerrada = db.Column(db.Integer, nullable=False, default=0)
    guias_solo_en_fils = db.Column(db.Integer, nullable=False, default=0)
    guias_solo_en_naviera = db.Column(db.Integer, nullable=False, default=0)

    total_fils = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    total_naviera = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    diferencia_global = db.Column(db.Numeric(14, 2), nullable=False, default=0)

    naviera = db.Column(db.String(30), nullable=False)  # ONE / COSCO