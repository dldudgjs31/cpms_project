from sched import scheduler

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from app.models import Cps_d_agg, Cps_sub_m_agg
from app.utils.db import db
from app.utils.cps_func import CoupangAPI
from app.models.user_model import User
from datetime import datetime

# 쿠팡파트너스 일 실적 집계
def job_cps_d_agg():
    with scheduler.app_context():
        print("일일 데이터 집계 START....")
        users = db.session.query(
            User.id,
            User.cps_access_key,
            User.cps_secret_key,
            User.cps_chn_id
        ).filter(
            User.cps_access_key.isnot(None),
            User.cps_secret_key.isnot(None)
        ).all()
        result = [
            {
                "id": user.id,
                "cps_access_key": user.cps_access_key,
                "cps_secret_key": user.cps_secret_key,
                "cps_chn_id": user.cps_chn_id
            } for user in users
        ]

        for member in result:
            coupang_api = CoupangAPI(access_key=member['cps_access_key'], secret_key=member['cps_secret_key'])
            data = coupang_api.get_cps_d_agg('20240717', '20240717')
            for chn in data:
                new_cps_d_agg = Cps_d_agg(
                    usr_id=chn.get('id')
                    , cps_chn_id=chn.get('trackingCode')
                    , cps_sub_id=chn.get('subId')
                    , make_dt=chn.get('date')
                    , click_cnt=chn.get('click')
                    , order_cnt=chn.get('order')
                    , cancel_cnt=chn.get('cancel')
                    , total_amt=chn.get('gmv')
                    , total_cms=chn.get('commission')
                )
                try:
                    db.session.add(new_cps_d_agg)
                    db.session.commit()
                    db.session.close()
                except IntegrityError:
                    db.session.rollback()
                    db.session.close()
        print("일일 데이터 집계 END....")

# 쿠팡파트너스 월 실적 집계
def job_cps_sub_m_agg():
    with scheduler.app_context():
        print("SUB채널 월별 데이터 집계 START....")
        results = db.session.query(
            Cps_d_agg.usr_id,
            Cps_d_agg.cps_chn_id,
            Cps_d_agg.cps_sub_id,
            func.date_format(Cps_d_agg.make_dt, '%Y%m').label('make_yymm'),
            func.coalesce(func.sum(Cps_d_agg.click_cnt), 0).label('total_clicks'),
            func.coalesce(func.sum(Cps_d_agg.order_cnt), 0).label('total_orders'),
            func.coalesce(func.sum(Cps_d_agg.cancel_cnt), 0).label('total_cancels'),
            func.coalesce(func.sum(Cps_d_agg.total_amt), 0).label('total_amount'),
            func.coalesce(func.sum(Cps_d_agg.total_cms), 0).label('total_commission')
        ).group_by(
            Cps_d_agg.usr_id,
            Cps_d_agg.cps_chn_id,
            Cps_d_agg.cps_sub_id,
            func.date_format(Cps_d_agg.make_dt, '%Y%m')
        ).all()
        summary = [
            {
                "user_id": result.usr_id,
                "cps_chn_id": result.cps_chn_id,
                "cps_sub_id": result.cps_sub_id,
                "make_yymm": result.make_yymm,
                "total_clicks": result.total_clicks,
                "total_orders": result.total_orders,
                "total_cancels": result.total_cancels,
                "total_amount": result.total_amount,
                "total_commission": result.total_commission
            } for result in results
        ]

        for chn in summary:
            new_sub_cps_m_agg = Cps_sub_m_agg(
                usr_id=chn['user_id']
                , cps_chn_id=chn['cps_chn_id']
                , cps_sub_id=chn['cps_sub_id']
                , make_yymm=chn['make_yymm']
                , click_cnt=chn['total_clicks']
                , order_cnt=chn['total_orders']
                , cancel_cnt=chn['total_cancels']
                , total_amt=chn['total_amount']
                , total_cms=chn['total_commission']
                , ins_day=datetime.utcnow()
            )
            try:
                db.session.add(new_sub_cps_m_agg)
                db.session.commit()
                db.session.close()
            except IntegrityError:
                db.session.rollback()
                db.session.close()
        print("SUB채널 월별 데이터 집계 END....")
def create_scheduler(app):
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=job_cps_d_agg, trigger=CronTrigger(hour=0, minute=0))  # 매일 자정에 실행
    scheduler.add_job(func=job_cps_sub_m_agg, trigger=CronTrigger(hour=0, minute=30))
    scheduler.app = app
    return scheduler
