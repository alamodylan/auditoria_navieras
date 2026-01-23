# app/models/job.py

from datetime import datetime
from app.extensions import db

class Job(db.Model):
    __tablename__ = "jobs"

    id = db.Column(db.Integer, primary_key=True)
    status = db.Column(db.String(30), nullable=False, default="CREATED")
    naviera = db.Column(db.String(30), nullable=False, default="COSCO")

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    finished_at = db.Column(db.DateTime)

    error_message = db.Column(db.Text)

    def mark_running(self):
        self.status = "RUNNING"

    def mark_done(self):
        self.status = "DONE"
        self.finished_at = datetime.utcnow()

    def mark_failed(self, error):
        self.status = "FAILED"
        self.error_message = str(error)
        self.finished_at = datetime.utcnow()