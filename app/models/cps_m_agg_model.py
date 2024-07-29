from datetime import datetime
from app.utils.db import db

class Cps_m_agg(db.Model):
    __tablename__ = 'T_CPS_M_AGG'  # 테이블명 지정
    usr_id = db.Column(db.Integer, primary_key=True, comment='사용자ID')
    cps_chn_id = db.Column(db.String(50), primary_key=True, comment='쿠팡파트너스 ID')
    make_yymm = db.Column(db.String(6),primary_key=True, comment='연월')
    click_cnt = db.Column(db.Integer, default=0, comment='클릭수')
    order_cnt = db.Column(db.Integer,default=0, comment='주문수')
    cancel_cnt = db.Column(db.Integer,default=0, comment='취소수')
    total_amt = db.Column(db.Integer,default=0, comment='판매액')
    total_cms = db.Column(db.Integer,default=0, comment='수수료')
    ins_day = db.Column(db.DateTime, default=datetime.utcnow)
    upt_day = db.Column(db.DateTime)
    del_day = db.Column(db.DateTime)
