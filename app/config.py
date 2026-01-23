# app/config.py

import os

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key")

    # PostgreSQL (Render / local)
    # Render a veces entrega DATABASE_URL como postgres:// (deprecated)
    uri = os.getenv("DATABASE_URL", "postgresql://localhost/auditoria_navieras")
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)

    # Forzar driver pg8000 (para evitar psycopg2 en Render)
    # Si ya viene con driver, no lo tocamos
    if uri.startswith("postgresql://") and "+pg8000" not in uri:
        uri = uri.replace("postgresql://", "postgresql+pg8000://", 1)

    SQLALCHEMY_DATABASE_URI = uri
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Rutas de archivos
    UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER", "uploads")
    OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER", "outputs")

    # Auditoría
    MONEY_TOLERANCE = float(os.getenv("MONEY_TOLERANCE", "1.00"))

    # Limite upload (50MB)
    MAX_CONTENT_LENGTH = 50 * 1024 * 1024