from datetime import datetime
from app.utils.db import db

class Cps_sub_m_agg(db.Model):
    __tablename__ = 'T_CPS_SUB_M_AGG'  # 테이블명 지정
    usr_id = db.Column(db.Integer, primary_key=True, autoincrement=False)
    cps_chn_id = db.Column(db.String(50), primary_key=True)
    cps_sub_id = db.Column(db.String(50), primary_key=True)
    make_yymm = db.Column(db.String(6),primary_key=True)
    click_cnt = db.Column(db.Integer, default=0)
    order_cnt = db.Column(db.Integer,default=0)
    cancel_cnt = db.Column(db.Integer,default=0)
    total_amt = db.Column(db.Integer,default=0)
    total_cms = db.Column(db.Integer,default=0)
    ins_day = db.Column(db.DateTime, default=datetime.utcnow)
    upt_day = db.Column(db.DateTime)
    del_day = db.Column(db.DateTime)
