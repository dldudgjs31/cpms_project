from flask import Flask
from app.config import Config
from app.routes import init_routes
from app.utils.db import init_db
from app.scheduler import create_scheduler

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    init_db(app)
    init_routes(app)

    scheduler = create_scheduler(app)
    scheduler.start()
    return app
