"""
Initializes the Flask application and registers the blueprints.
"""
from flask import Flask
from app.routes.dashboard import dashboard_bp
from app.routes.api import api_bp

def create_app():
    app = Flask(__name__)

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(api_bp)

    return app