# scripts/seed_dev.py

from app import create_app
from app.extensions import db
from app.models import Job

app = create_app()

with app.app_context():
    j = Job(naviera="COSCO", status="CREATED")
    db.session.add(j)
    db.session.commit()
    print("Job creado:", j.id)