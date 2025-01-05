# app/__init__.py
from flask import Flask
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from apscheduler.schedulers.background import BackgroundScheduler
from config import Config

db = SQLAlchemy()
migrate = Migrate()
# Initialize scheduler as None first
scheduler = None

def create_app(config_class=Config):
    app = Flask(__name__)
    CORS(app)  # Enable CORS
    app.config.from_object(config_class)


    # Initialize extensions
    db.init_app(app)
    migrate.init_app(app, db)

    # Initialize scheduler only if it's not running
    global scheduler
    if scheduler is None or not scheduler.running:
        scheduler = BackgroundScheduler()
        scheduler.configure(misfire_grace_time=None)
        scheduler.start()

    # Register blueprints
    from app.api import bp as api_bp
    app.register_blueprint(api_bp, url_prefix='/api')

    return app