from .home import home_bp
from .test import test_bp  # 새로운 라우트 추가

def init_routes(app):
    app.register_blueprint(home_bp)
    app.register_blueprint(test_bp)  # 새로운 라우트 추가
