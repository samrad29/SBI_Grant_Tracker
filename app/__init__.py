"""
Initializes the Flask application and registers the blueprints.
"""
import os

from flask import Flask

from app.routes.api import api_bp
from app.routes.auth import auth_bp
from app.routes.dashboard import dashboard_bp
from app.routes.db_migration import db_migration_bp
from app.routes.user_activity import user_activity_bp

from flask_cors import CORS


def _env_bool(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def create_app():
    app = Flask(__name__)
    app.secret_key = os.getenv("SECRET_KEY", "dev-secret-change-in-production")

    # Session cookie (Flask signed cookie). Defaults: SameSite=Lax, Secure=False — fine for
    # same-origin (e.g. API + templates on one host). For a separate-site frontend (Vercel)
    # calling this API with credentials, set on the API host:
    #   SESSION_CROSS_SITE_COOKIES=1
    # which sets SameSite=None + Secure=True (HTTPS only; required for cross-site cookies).
    if _env_bool("SESSION_CROSS_SITE_COOKIES"):
        app.config.update(
            SESSION_COOKIE_SAMESITE="None",
            SESSION_COOKIE_SECURE=True,
        )
    ss = (os.getenv("SESSION_COOKIE_SAMESITE") or "").strip()
    if ss:
        app.config["SESSION_COOKIE_SAMESITE"] = None if ss.lower() == "none" else ss
    sec_raw = (os.getenv("SESSION_COOKIE_SECURE") or "").strip().lower()
    if sec_raw in {"1", "true", "yes", "on"}:
        app.config["SESSION_COOKIE_SECURE"] = True
    elif sec_raw in {"0", "false", "no"}:
        app.config["SESSION_COOKIE_SECURE"] = False

    CORS(
        app,
        origins=["https://sbigrants.vercel.app", "portal.sunbearindustries.com"],
        supports_credentials=True,  # needed if you use sessions/cookies
    )

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(user_activity_bp)
    app.register_blueprint(db_migration_bp)
    return app