# app/blueprints/web/forms.py

from flask_wtf import FlaskForm
from wtforms import SelectField, SubmitField
from flask_wtf.file import FileField, FileRequired, FileAllowed

class UploadAuditForm(FlaskForm):
    naviera = SelectField(
        "Naviera",
        choices=[("ONE", "ONE"), ("COSCO", "COSCO")],
        default="COSCO",
    )

    archivo_facturacion = FileField(
        "Archivo Facturación (ONE o COSCO)",
        validators=[
            FileRequired(),
            FileAllowed(["xlsx"], "Solo se permiten archivos .xlsx"),
        ],
    )

    archivo_fils = FileField(
        "ReporteGuiaAuditoria (FILS)",
        validators=[
            FileRequired(),
            FileAllowed(["xlsx"], "Solo se permiten archivos .xlsx"),
        ],
    )

    submit = SubmitField("Pre-check")