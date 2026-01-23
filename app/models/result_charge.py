# app/models/result_charge.py

from app.extensions import db

class ResultCharge(db.Model):
    __tablename__ = "result_charge"

    id = db.Column(db.Integer, primary_key=True)
    job_id = db.Column(db.Integer, db.ForeignKey("jobs.id"), nullable=False)

    guia = db.Column(db.String(50), nullable=False, index=True)
    contenedor = db.Column(db.String(20), index=True)

    tipo_cargo = db.Column(db.String(120), nullable=False)
    monto = db.Column(db.Numeric(14, 2), nullable=False, default=0)

    origen = db.Column(db.String(20), nullable=False, default="FILS")  # FILS / NAVIERA
    naviera = db.Column(db.String(30), nullable=False)  # ONE / COSCO