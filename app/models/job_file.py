# app/models/job_file.py

from app.extensions import db


class JobFile(db.Model):
    __tablename__ = "job_files"
    __table_args__ = {"schema": "auditoria"}

    id = db.Column(db.Integer, primary_key=True)

    job_id = db.Column(
        db.Integer,
        db.ForeignKey("auditoria.jobs.id"),
        nullable=False
    )

    original_name = db.Column(db.String(255), nullable=False)
    stored_path = db.Column(db.String(500), nullable=False)
    file_type = db.Column(db.String(20), nullable=False)  # FILS / ONE / COSCO
    file_hash = db.Column(db.String(64), nullable=False)

    job = db.relationship("Job", backref=db.backref("files", lazy=True))