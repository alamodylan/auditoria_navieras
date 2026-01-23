# app/blueprints/health/routes.py

from flask import Blueprint, jsonify

health_bp = Blueprint("health", __name__)

@health_bp.route("/")
def health():
    return jsonify({"status": "healthy"})