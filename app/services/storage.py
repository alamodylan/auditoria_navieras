# app/services/storage.py

import os
import hashlib
from werkzeug.utils import secure_filename

from app.utils.logging import get_logger

logger = get_logger("storage")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def sha256_file(path: str, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def save_uploaded_file(file_storage, base_upload_folder: str, job_id: int, file_type: str) -> dict:
    """
    Guarda archivo subido en: uploads/<job_id>/<file_type>/archivo.xlsx
    Retorna:
      {
        original_name, stored_path, file_hash, file_type
      }
    """
    if not file_storage:
        raise ValueError("No file provided")

    original_name = file_storage.filename or "archivo.xlsx"
    safe_name = secure_filename(original_name)

    job_folder = os.path.join(base_upload_folder, str(job_id), file_type.upper())
    ensure_dir(job_folder)

    stored_path = os.path.join(job_folder, safe_name)
    file_storage.save(stored_path)

    file_hash = sha256_file(stored_path)

    logger.info(f"Saved file job={job_id} type={file_type} name={original_name} hash={file_hash}")

    return {
        "original_name": original_name,
        "stored_path": stored_path,
        "file_hash": file_hash,
        "file_type": file_type.upper(),
    }