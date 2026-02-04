# app/worker.py

import os
import time
import signal
from sqlalchemy import text

from app import create_app
from app.extensions import db
from app.models import Job
from app.services.job_runner import run_job


STOP = False


def _handle_stop(signum, frame):
    global STOP
    STOP = True
    print(f"🛑 Señal recibida ({signum}). Cerrando worker con gracia...")


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
    # Señales típicas en Render al detener/redeploy
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    print("🚀 Worker iniciado. Esperando jobs...")

    app = create_app()

    # Configs (puedes cambiarlos por env vars en Render)
    poll_seconds = int(os.getenv("WORKER_POLL_SECONDS", "3"))
    money_tolerance = float(os.getenv("MONEY_TOLERANCE", "1.0"))
    output_folder = os.getenv("OUTPUT_FOLDER", "outputs")

    with app.app_context():
        print("✅ App context OK. Entrando al loop infinito.")

        while not STOP:
            try:
                job = _fetch_next_job()

                if not job:
                    time.sleep(poll_seconds)
                    continue

                print(f"🧾 Job encontrado: id={job.id}. Marcando RUNNING...")
                job.mark_running()
                job.error_message = None
                db.session.commit()

                t0 = time.time()

                # Ejecutar
                result = run_job(
                    job_id=job.id,
                    money_tolerance=money_tolerance,
                    output_folder=output_folder,
                )

                elapsed = time.time() - t0
                status = (result or {}).get("status", "UNKNOWN")

                print(f"✅ Job {job.id} terminó. status={status} elapsed={elapsed:.1f}s")

                # run_job ya marca DONE o FAILED internamente,
                # pero por seguridad, si viniera algo raro:
                if status == "FAILED" and job.status != "FAILED":
                    job.mark_failed((result or {}).get("error", "Job falló sin detalle."))
                    db.session.commit()

            except Exception as e:
                print(f"❌ Error en worker: {type(e).__name__}: {e}")

                try:
                    db.session.rollback()
                except Exception:
                    pass

                # Evitar loop súper rápido en caso de error persistente
                time.sleep(max(poll_seconds, 3))

            finally:
                # MUY importante en procesos infinitos: limpiar sesión al final de cada vuelta
                try:
                    db.session.remove()
                except Exception:
                    pass

    print("👋 Worker detenido.")


if __name__ == "__main__":
    main()