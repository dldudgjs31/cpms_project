from flask import Flask
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from app.config import Config
from app.routes import init_routes
from app.utils.db import db, init_db
from app.scheduler import create_scheduler

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # SQLAlchemy 연결 풀 설정
    engine = create_engine(
        app.config['SQLALCHEMY_DATABASE_URI'],
        pool_size=10,  # 기본 연결 풀 크기
        max_overflow=20,  # 기본 풀을 초과하여 생성할 수 있는 연결 수
        pool_timeout=30,  # 풀에서 연결을 가져오기 전에 대기할 시간(초)
        pool_recycle=3600  # 연결이 재활용되기 전에 대기할 시간(초)
    )

    # 세션을 사용하도록 설정
    db.session = scoped_session(sessionmaker(bind=engine))

    # Flask 애플리케이션과 SQLAlchemy 인스턴스 연동
    db.init_app(app)

    with app.app_context():
        db.create_all()

    init_routes(app)
    scheduler = create_scheduler(app)

    return app
