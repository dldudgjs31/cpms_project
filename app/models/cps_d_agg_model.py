from datetime import datetime
from app.utils.db import db

class Cps_d_agg(db.Model):
    __tablename__ = 'T_CPS_D_AGG'  # 테이블명 지정
    '''
      - 유저ID
      - 채널ID
      - 수익일자 YYYYMMDD
      - 클릭건수
      - 구매건수
      - 취소건수
      - 총 판매액
      - 총 수익(커미션)
      "date": "20190307",
      "trackingCode": "AF1234567",
      "subId": "A1234567890",
      "commission": 267,
      "click": 888,
      "order": 888,
      "cancel": 888,
      "gmv": 8900
    '''
    usr_id = db.Column(db.Integer, primary_key=True)
    cps_chn_id = db.Column(db.String(50), primary_key=True,comment='쿠팡파트너스ID')
    cps_sub_id = db.Column(db.String(50), primary_key=True,comment='쿠팡파트너스 서브 채널ID')
    make_dt = db.Column(db.String(8),primary_key=True)
    click_cnt = db.Column(db.Integer, default=0, comment='클릭수')
    order_cnt = db.Column(db.Integer,default=0, comment='주문수')
    cancel_cnt = db.Column(db.Integer,default=0, comment='취소수')
    total_amt = db.Column(db.Integer,default=0, comment='판매액')
    total_cms = db.Column(db.Integer,default=0, comment='수수료')
    ins_day = db.Column(db.DateTime, default=datetime.utcnow)
    upt_day = db.Column(db.DateTime)
    del_day = db.Column(db.DateTime)
