# app/models/result_container.py

from app.extensions import db

class ResultContainer(db.Model):
    __tablename__ = "result_container"

    id = db.Column(db.Integer, primary_key=True)
    job_id = db.Column(db.Integer, db.ForeignKey("jobs.id"), nullable=False)

    guia = db.Column(db.String(50), nullable=False, index=True)
    contenedor = db.Column(db.String(20), nullable=False, index=True)

    ruta = db.Column(db.String(80))
    flete = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    extras = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    total = db.Column(db.Numeric(14, 2), nullable=False, default=0)

    naviera = db.Column(db.String(30), nullable=False)  # ONE / COSCO