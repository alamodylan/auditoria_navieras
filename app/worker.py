# app/worker.py

import os
import time
from sqlalchemy import text

from app import create_app
from app.extensions import db
from app.models import Job
from app.services.job_runner import run_job


def _set_search_path():
    # Blindaje para que SIEMPRE use auditoria schema en esta conexión
    db.session.execute(text("SET search_path TO auditoria, public;"))


def _fetch_next_job() -> Job | None:
    """
    Toma el siguiente job encolado.
    Orden: más viejo primero.
    """
    _set_search_path()
    return (
        Job.query
        .filter(Job.status == "QUEUED")
        .order_by(Job.id.asc())
        .first()
    )


def main():
    app = create_app()

    # Configs (puedes cambiarlos por env vars en Render)
    poll_seconds = int(os.getenv("WORKER_POLL_SECONDS", "3"))
    money_tolerance = float(os.getenv("MONEY_TOLERANCE", "1.0"))
    output_folder = os.getenv("OUTPUT_FOLDER", "outputs")

    with app.app_context():
        while True:
            try:
                job = _fetch_next_job()

                if not job:
                    time.sleep(poll_seconds)
                    continue

                # Marcar RUNNING
                job.mark_running()
                job.error_message = None
                db.session.commit()

                # Ejecutar
                result = run_job(
                    job_id=job.id,
                    money_tolerance=money_tolerance,
                    output_folder=output_folder,
                )

                # run_job ya marca DONE o FAILED internamente,
                # pero por seguridad, si viniera algo raro:
                if result.get("status") == "FAILED" and job.status != "FAILED":
                    job.mark_failed(result.get("error", "Job falló sin detalle."))
                    db.session.commit()

            except Exception as e:
                # Si algo revienta a nivel worker, no lo mates: log y sigue.
                try:
                    db.session.rollback()
                except Exception:
                    pass

                # Evitar loop súper rápido en caso de error persistente
                time.sleep(max(poll_seconds, 3))


if __name__ == "__main__":
    main()