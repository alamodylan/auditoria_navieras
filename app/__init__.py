# app/__init__.py

from flask import Flask
from .config import Config
from .extensions import db, migrate

def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    # Inicializar extensiones
    db.init_app(app)
    migrate.init_app(app, db)

    # Registrar blueprints
    from .blueprints.web.routes import web_bp
    from .blueprints.api.routes import api_bp
    from .blueprints.health.routes import health_bp

    app.register_blueprint(web_bp)
    app.register_blueprint(api_bp, url_prefix="/api")
    app.register_blueprint(health_bp, url_prefix="/health")

    return app