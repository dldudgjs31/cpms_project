from datetime import datetime
from app.utils.db import db

class User(db.Model):
    __tablename__ = 'T_USR'  # 테이블명 지정
    id = db.Column(db.Integer, autoincrement=True, primary_key=True)
    nm = db.Column(db.String(50), nullable=True)
    login_id = db.Column(db.String(50), nullable=True)
    login_pwd = db.Column(db.String(20), nullable=True)
    cps_chn_id = db.Column(db.String(100), nullable=True)
    cps_access_key = db.Column(db.String(100), nullable=True)
    cps_secret_key = db.Column(db.String(100), nullable=True)
    ggle_api_key = db.Column(db.String(100), nullable=True)
    ggle_secret_file_path = db.Column(db.String(100), nullable=True)
    ins_day = db.Column(db.DateTime, nullable=True, default=datetime.utcnow)
    upt_day = db.Column(db.DateTime, nullable=True)
    del_day = db.Column(db.DateTime, nullable=True)
