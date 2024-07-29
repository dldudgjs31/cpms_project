from datetime import datetime
from app.utils.db import db

class Cps_sub_w_agg(db.Model):
    __tablename__ = 'T_CPS_SUB_W_AGG'  # 테이블명 지정
    usr_id = db.Column(db.Integer, primary_key=True)
    cps_chn_id = db.Column(db.String(50), primary_key=True,comment='쿠팡파트너스ID')
    cps_sub_id = db.Column(db.String(50), primary_key=True,comment='쿠팡파트너스 서브 채널ID')
    start_dt = db.Column(db.String(8),primary_key=True)
    end_dt = db.Column(db.String(8), primary_key=True)
    week_seq = db.Column(db.Integer, primary_key=True)
    click_cnt = db.Column(db.Integer, default=0, comment='클릭수')
    order_cnt = db.Column(db.Integer,default=0, comment='주문수')
    cancel_cnt = db.Column(db.Integer,default=0, comment='취소수')
    total_amt = db.Column(db.Integer,default=0, comment='판매액')
    total_cms = db.Column(db.Integer,default=0, comment='수수료')
    ins_day = db.Column(db.DateTime, default=datetime.utcnow)
    upt_day = db.Column(db.DateTime)
    del_day = db.Column(db.DateTime)
