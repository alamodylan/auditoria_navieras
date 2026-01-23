# app/blueprints/web/routes.py

import os
from decimal import Decimal

from flask import (
    Blueprint, render_template, request, redirect, url_for,
    flash, send_file, current_app
)

from app.extensions import db
from app.models import (
    Job, JobFile,
    ResultSummary, ResultContainer, ResultCharge, ResultException, ResultKPI
)

from app.blueprints.web.forms import UploadAuditForm
from app.services.storage import save_uploaded_file
from app.services.precheck import run_precheck, report_to_dict
from app.services.job_runner import run_job


web_bp = Blueprint("web", __name__)


@web_bp.route("/")
def home():
    return render_template("home.html")


@web_bp.route("/upload", methods=["GET", "POST"])
def upload():
    form = UploadAuditForm()

    if request.method == "POST":
        if not form.validate_on_submit():
            flash("Formulario inválido. Verifique los archivos y campos.", "error")
            return render_template("upload.html", form=form)

        naviera = (form.naviera.data or "COSCO").upper().strip()

        # 1) Crear Job
        job = Job(naviera=naviera, status="CREATED")
        db.session.add(job)
        db.session.commit()

        # 2) Guardar archivos
        try:
            base_upload = current_app.config.get("UPLOAD_FOLDER", "uploads")

            # Facturación
            fact_file = form.archivo_facturacion.data
            fact_saved = save_uploaded_file(
                fact_file, base_upload_folder=base_upload, job_id=job.id, file_type=naviera
            )
            db.session.add(JobFile(
                job_id=job.id,
                original_name=fact_saved["original_name"],
                stored_path=fact_saved["stored_path"],
                file_type=fact_saved["file_type"],
                file_hash=fact_saved["file_hash"],
            ))

            # FILS
            fils_file = form.archivo_fils.data
            fils_saved = save_uploaded_file(
                fils_file, base_upload_folder=base_upload, job_id=job.id, file_type="FILS"
            )
            db.session.add(JobFile(
                job_id=job.id,
                original_name=fils_saved["original_name"],
                stored_path=fils_saved["stored_path"],
                file_type=fils_saved["file_type"],
                file_hash=fils_saved["file_hash"],
            ))

            db.session.commit()

        except Exception as e:
            job.status = "FAILED"
            job.error_message = f"Error guardando archivos: {e}"
            db.session.commit()
            flash("No se pudieron guardar los archivos. Ver detalle en el job.", "error")
            return redirect(url_for("web.job_detail", job_id=job.id))

        # 3) Pre-check
        try:
            files = {f.file_type.upper(): f for f in job.files}
            fils_path = files["FILS"].stored_path
            fact_path = files[naviera].stored_path

            report = run_precheck(naviera=naviera, fils_path=fils_path, facturacion_path=fact_path)
            job.status = "PRECHECK_OK" if report.ok else "PRECHECK_FAIL"
            db.session.commit()

            return render_template(
                "precheck.html",
                job_id=job.id,
                report=report_to_dict(report),
            )

        except Exception as e:
            job.status = "FAILED"
            job.error_message = f"Pre-check falló: {e}"
            db.session.commit()
            flash("Pre-check falló. Ver detalle en el job.", "error")
            return redirect(url_for("web.job_detail", job_id=job.id))

    return render_template("upload.html", form=form)


@web_bp.route("/jobs")
def jobs():
    jobs = Job.query.order_by(Job.id.desc()).limit(200).all()
    return render_template("jobs.html", jobs=jobs)


@web_bp.route("/job/<int:job_id>")
def job_detail(job_id: int):
    job = Job.query.get_or_404(job_id)
    return render_template("job_detail.html", job=job)


@web_bp.route("/job/<int:job_id>/run", methods=["POST"])
def run_job_route(job_id: int):
    job = Job.query.get_or_404(job_id)

    # Requisito: precheck ok
    if job.status not in ("PRECHECK_OK", "CREATED"):
        flash(f"No se puede ejecutar. Estado actual: {job.status}", "error")
        return redirect(url_for("web.job_detail", job_id=job_id))

    tol = float(current_app.config.get("MONEY_TOLERANCE", 1.00))
    output_folder = current_app.config.get("OUTPUT_FOLDER", "outputs")

    result = run_job(job_id=job_id, money_tolerance=tol, output_folder=output_folder)

    if result.get("status") == "DONE":
        flash("Job ejecutado correctamente.", "success")
        return redirect(url_for("web.results", job_id=job_id))

    flash(f"Job falló: {result.get('error')}", "error")
    return redirect(url_for("web.job_detail", job_id=job_id))


@web_bp.route("/job/<int:job_id>/results")
def results(job_id: int):
    job = Job.query.get_or_404(job_id)

    if job.status != "DONE":
        flash("El job aún no está finalizado.", "warn")
        return redirect(url_for("web.job_detail", job_id=job_id))

    # Cargar resultados desde DB
    kpi = ResultKPI.query.filter_by(job_id=job_id).first()
    resumen = ResultSummary.query.filter_by(job_id=job_id).order_by(ResultSummary.guia.asc()).all()
    contenedores = ResultContainer.query.filter_by(job_id=job_id).order_by(ResultContainer.guia.asc()).all()
    cargos = ResultCharge.query.filter_by(job_id=job_id).order_by(ResultCharge.guia.asc()).all()
    excepciones = ResultException.query.filter_by(job_id=job_id).order_by(ResultException.severidad.desc()).all()

    return render_template(
        "result_tables.html",
        job=job,
        kpi=kpi,
        resumen=resumen,
        contenedores=contenedores,
        cargos=cargos,
        excepciones=excepciones,
    )


@web_bp.route("/job/<int:job_id>/download")
def download_export(job_id: int):
    """
    Descarga el Excel exportado (outputs/<job_id>/Auditoria_<job_id>.xlsx)
    """
    job = Job.query.get_or_404(job_id)
    if job.status != "DONE":
        flash("El job no está en estado DONE, no hay export disponible.", "error")
        return redirect(url_for("web.job_detail", job_id=job_id))

    output_folder = current_app.config.get("OUTPUT_FOLDER", "outputs")
    export_path = os.path.join(output_folder, str(job_id), f"Auditoria_{job_id}.xlsx")

    if not os.path.exists(export_path):
        flash("No se encontró el archivo exportado. Reintente ejecutar el job.", "error")
        return redirect(url_for("web.results", job_id=job_id))

    return send_file(export_path, as_attachment=True, download_name=f"Auditoria_{job_id}.xlsx")