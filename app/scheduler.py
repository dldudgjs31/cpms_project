from operator import and_
from sched import scheduler

from MySQLdb.constants.FIELD_TYPE import INTERVAL
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import func, extract, case
from sqlalchemy.exc import IntegrityError

from app.models import Cps_d_agg, Cps_sub_m_agg, Cps_m_agg, Cps_w_agg, Cps_sub_w_agg
from app.utils.db import db
from app.utils.cps_func import CoupangAPI
from app.models.user_model import User
from datetime import datetime, timedelta

# 쿠팡파트너스 일 실적 집계1
def job_cps_d_agg(app):
    with app.app_context():
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
        # 현재 일자
        today = datetime.now()
        # 어제 일자
        today = today - timedelta(days=1)
        yesterday = today - timedelta(days=1)
        # yyyymmdd 형식으로 출력
        today_str = today.strftime('%Y%m%d')
        yesterday_str = yesterday.strftime('%Y%m%d')
        try:
            db.session.query(Cps_d_agg).filter(
                and_(
                    Cps_d_agg.make_dt >= '20240701',
                    Cps_d_agg.make_dt <= '20240728'
                )
            ).delete(synchronize_session=False)
            db.session.commit()
            print("Data deletion completed.")
        except IntegrityError:
            db.session.rollback()
            print("Data deletion failed due to integrity error.")
        finally:
            db.session.close()

        for member in result:
            coupang_api = CoupangAPI(access_key=member['cps_access_key'], secret_key=member['cps_secret_key'])
            data = coupang_api.get_cps_d_agg('20240701', '20240728')
            for chn in data:
                new_cps_d_agg = Cps_d_agg(
                    usr_id=member['id']
                    , cps_chn_id=chn.get('trackingCode')
                    , cps_sub_id=chn.get('subId', '기타')
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
                except IntegrityError:
                    db.session.rollback()
                finally:
                    db.session.close()
        print("일일 데이터 집계 END....")

# 쿠팡파트너스 월 실적 집계 2
def job_cps_sub_m_agg(app):
    with app.app_context():
        print("SUB채널 월별 데이터 집계 START....")
        # 현재 날짜와 어제 날짜
        today = datetime.now()
        yesterday = today - timedelta(days=1)

        # 현재 연월과 어제 연월을 문자열로 변환
        current_month_str = today.strftime('%Y%m')
        previous_month_str = yesterday.strftime('%Y%m')
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
        ).filter(
            func.date_format(Cps_d_agg.make_dt, '%Y%m').in_([current_month_str, previous_month_str])
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
            except IntegrityError:
                db.session.rollback()
            finally:
                db.session.close()
        print("SUB채널 월별 데이터 집계 END....")

# 쿠팡파트너스 월 실적 집계 3
def job_cps_m_agg(app):
    with app.app_context():
        print("전체 채널별 월별 데이터 집계 START....")
        # 현재 날짜와 어제 날짜
        today = datetime.now()
        yesterday = today - timedelta(days=1)

        # 현재 연월과 어제 연월을 문자열로 변환
        current_month_str = today.strftime('%Y%m')
        previous_month_str = yesterday.strftime('%Y%m')
        results = db.session.query(
            Cps_d_agg.usr_id,
            Cps_d_agg.cps_chn_id,
            func.date_format(Cps_d_agg.make_dt, '%Y%m').label('make_yymm'),
            func.coalesce(func.sum(Cps_d_agg.click_cnt), 0).label('total_clicks'),
            func.coalesce(func.sum(Cps_d_agg.order_cnt), 0).label('total_orders'),
            func.coalesce(func.sum(Cps_d_agg.cancel_cnt), 0).label('total_cancels'),
            func.coalesce(func.sum(Cps_d_agg.total_amt), 0).label('total_amount'),
            func.coalesce(func.sum(Cps_d_agg.total_cms), 0).label('total_commission')
        ).filter(
            func.date_format(Cps_d_agg.make_dt, '%Y%m').in_([current_month_str, previous_month_str])
        ).group_by(
            Cps_d_agg.usr_id,
            Cps_d_agg.cps_chn_id,
            func.date_format(Cps_d_agg.make_dt, '%Y%m')
        ).all()
        summary = [
            {
                "user_id": result.usr_id,
                "cps_chn_id": result.cps_chn_id,
                "make_yymm": result.make_yymm,
                "total_clicks": result.total_clicks,
                "total_orders": result.total_orders,
                "total_cancels": result.total_cancels,
                "total_amount": result.total_amount,
                "total_commission": result.total_commission
            } for result in results
        ]

        for chn in summary:
            new_cps_m_agg = Cps_m_agg(
                usr_id=chn['user_id']
                , cps_chn_id=chn['cps_chn_id']
                , make_yymm=chn['make_yymm']
                , click_cnt=chn['total_clicks']
                , order_cnt=chn['total_orders']
                , cancel_cnt=chn['total_cancels']
                , total_amt=chn['total_amount']
                , total_cms=chn['total_commission']
                , ins_day=datetime.utcnow()
            )
            try:
                db.session.add(new_cps_m_agg)
                db.session.commit()
            except IntegrityError:
                db.session.rollback()
            finally:
                db.session.close()
        print("전체 채널별 월별 데이터 집계 END....")

# 쿠팡파트너스 월 실적 집계 4
def job_cps_w_agg(app):
    with app.app_context():
        print("전체 채널별 주차별 데이터 집계 START....")
        results = db.session.query(
            Cps_d_agg.usr_id,
            Cps_d_agg.cps_chn_id,
            func.min(Cps_d_agg.make_dt).label('start_dt'),
            func.max(Cps_d_agg.make_dt).label('end_dt'),
            func.coalesce(func.sum(Cps_d_agg.click_cnt), 0).label('total_clicks'),
            func.coalesce(func.sum(Cps_d_agg.order_cnt), 0).label('total_orders'),
            func.coalesce(func.sum(Cps_d_agg.cancel_cnt), 0).label('total_cancels'),
            func.coalesce(func.sum(Cps_d_agg.total_amt), 0).label('total_amount'),
            func.coalesce(func.sum(Cps_d_agg.total_cms), 0).label('total_commission')
        ).group_by(
            Cps_d_agg.usr_id,
            Cps_d_agg.cps_chn_id,
            extract('year', Cps_d_agg.make_dt),
            extract('week', Cps_d_agg.make_dt)
        ).all()

        summary = []
        for result in results:
            start_dt = result.start_dt
            if isinstance(start_dt, str):
                start_dt = datetime.strptime(start_dt, "%Y%m%d")  # 문자열을 datetime 객체로 변환
            start_dt, end_dt = get_week_start_end(start_dt)  # 주의 시작일과 종료일 계산
            year, week, _ = start_dt.isocalendar()
            print(start_dt.isocalendar())
            summary.append({
                "user_id": result.usr_id,
                "cps_chn_id": result.cps_chn_id,
                "year": year,
                "week": week,
                "start_dt": start_dt,
                "end_dt": end_dt,
                "total_clicks": result.total_clicks,
                "total_orders": result.total_orders,
                "total_cancels": result.total_cancels,
                "total_amount": result.total_amount,
                "total_commission": result.total_commission
            })

        for chn in summary:
            new_cps_w_agg = Cps_w_agg(
                usr_id=chn['user_id'],
                cps_chn_id=chn['cps_chn_id'],
                week_seq=chn['week'],
                start_dt=chn['start_dt'].strftime("%Y%m%d"),
                end_dt=chn['end_dt'].strftime("%Y%m%d"),
                click_cnt=chn['total_clicks'],
                order_cnt=chn['total_orders'],
                cancel_cnt=chn['total_cancels'],
                total_amt=chn['total_amount'],
                total_cms=chn['total_commission'],
                ins_day=datetime.utcnow()
            )
            try:
                db.session.add(new_cps_w_agg)
                db.session.commit()
            except IntegrityError:
                db.session.rollback()
            finally:
                db.session.close()
        print("전체 채널별 주차별 데이터 집계 END....")

def get_week_start_end(date):
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y%m%d")  # 문자열을 datetime 객체로 변환
    start_of_week = date - timedelta(days=date.weekday())
    end_of_week = start_of_week + timedelta(days=6)
    return start_of_week, end_of_week
# 쿠팡파트너스 주 실적 집계
def job_cps_sub_w_agg(app):
    with app.app_context():
        print("서브 채널별 주차별 데이터 집계 START....")
        results = db.session.query(
            Cps_d_agg.usr_id,
            Cps_d_agg.cps_chn_id,
            Cps_d_agg.cps_sub_id,
            func.min(Cps_d_agg.make_dt).label('start_dt'),
            func.max(Cps_d_agg.make_dt).label('end_dt'),
            func.coalesce(func.sum(Cps_d_agg.click_cnt), 0).label('total_clicks'),
            func.coalesce(func.sum(Cps_d_agg.order_cnt), 0).label('total_orders'),
            func.coalesce(func.sum(Cps_d_agg.cancel_cnt), 0).label('total_cancels'),
            func.coalesce(func.sum(Cps_d_agg.total_amt), 0).label('total_amount'),
            func.coalesce(func.sum(Cps_d_agg.total_cms), 0).label('total_commission')
        ).group_by(
            Cps_d_agg.usr_id,
            Cps_d_agg.cps_chn_id,
            Cps_d_agg.cps_sub_id,
            extract('year', Cps_d_agg.make_dt),
            extract('week', Cps_d_agg.make_dt)
        ).all()

        summary = []
        for result in results:
            start_dt = result.start_dt
            if isinstance(start_dt, str):
                start_dt = datetime.strptime(start_dt, "%Y%m%d")  # 문자열을 datetime 객체로 변환
            start_dt, end_dt = get_week_start_end(start_dt)  # 주의 시작일과 종료일 계산
            year, week, _ = start_dt.isocalendar()
            print(start_dt.isocalendar())
            summary.append({
                "user_id": result.usr_id,
                "cps_chn_id": result.cps_chn_id,
                "cps_sub_id": result.cps_sub_id,
                "year": year,
                "week": week,
                "start_dt": start_dt,
                "end_dt": end_dt,
                "total_clicks": result.total_clicks,
                "total_orders": result.total_orders,
                "total_cancels": result.total_cancels,
                "total_amount": result.total_amount,
                "total_commission": result.total_commission
            })

        for chn in summary:
            new_cps_sub_w_agg = Cps_sub_w_agg(
                usr_id=chn['user_id'],
                cps_chn_id=chn['cps_chn_id'],
                cps_sub_id=chn['cps_sub_id'],
                week_seq=chn['week'],
                start_dt=chn['start_dt'].strftime("%Y%m%d"),
                end_dt=chn['end_dt'].strftime("%Y%m%d"),
                click_cnt=chn['total_clicks'],
                order_cnt=chn['total_orders'],
                cancel_cnt=chn['total_cancels'],
                total_amt=chn['total_amount'],
                total_cms=chn['total_commission'],
                ins_day=datetime.utcnow()
            )
            try:
                db.session.add(new_cps_sub_w_agg)
                db.session.commit()
            except IntegrityError:
                db.session.rollback()
            finally:
                db.session.close()
        print("서브 채널별 주차별 데이터 집계 END....")
def create_scheduler(app):
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=job_cps_d_agg, trigger=CronTrigger(hour=1, minute=46), args=[app])  # 매일 20:15에 실행
    scheduler.add_job(func=job_cps_sub_m_agg, trigger=CronTrigger(hour=1, minute=47), args=[app])  # 매월 0:30에 실행
    scheduler.add_job(func=job_cps_m_agg, trigger=CronTrigger(hour=1, minute=48), args=[app])  # 매월 0:30에 실행
    scheduler.add_job(func=job_cps_w_agg, trigger=CronTrigger(hour=1, minute=49), args=[app])  # 매월 0:30에 실행
    scheduler.add_job(func=job_cps_sub_w_agg, trigger=CronTrigger(hour=1, minute=50), args=[app])  # 매월 0:30에 실행
    scheduler.start()
    return scheduler


