from datetime import datetime
from flask import Blueprint, request, jsonify
from sqlalchemy.exc import IntegrityError

from app.models import Cps_d_agg, Cps_sub_m_agg
from app.models.user_model import User
from app.utils.db import db
from sqlalchemy import text, extract, func

test_bp = Blueprint('test', __name__)

@test_bp.route('/test', methods=['GET'])
def test():
    raw_sql = """
    SELECT USER_ID, USER_PWD
    FROM T_CHATGPT_USER
    """
    results = db.session.execute(text(raw_sql)).fetchall()
    print(results)
    return jsonify({"message": "hi"})

@test_bp.route('/test2', methods=['GET'])
def test2():
    #data = request.get_json()
    new_user = User(
        nm='이영헌',
        login_id='asdudgjs',
        login_pwd='test',
        cps_chn_id='af111111',
        cps_access_key='asdfasdfafasfasdf',
        cps_secret_key='asdfasdfafasfasdfw',
        ggle_api_key='asdfasdfafasfasdf22',
        ggle_secret_file_path='asdfasdfafasfasdf2222',
        ins_day=datetime.utcnow(),
        upt_day=None,
        del_day=None  # or `datetime.utcnow()` if you want to set a default deletion date
    )
    db.session.add(new_user)
    db.session.commit()
    return jsonify({"message": "User added successfully"}), 201

@test_bp.route('/monthly_summary', methods=['GET'])
def monthly_summary():
    results = db.session.query(
        Cps_d_agg.usr_id,
        Cps_d_agg.cps_chn_id,
        Cps_d_agg.cps_sub_id,
        func.date_format(Cps_d_agg.make_dt, '%Y%m').label('make_yymm'),
        func.coalesce(func.sum(Cps_d_agg.click_cnt),0).label('total_clicks'),
        func.coalesce(func.sum(Cps_d_agg.order_cnt),0).label('total_orders'),
        func.coalesce(func.sum(Cps_d_agg.cancel_cnt),0).label('total_cancels'),
        func.coalesce(func.sum(Cps_d_agg.total_amt),0).label('total_amount'),
        func.coalesce(func.sum(Cps_d_agg.total_cms),0).label('total_commission')
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
            , ins_day = datetime.utcnow()
        )
        try:
            db.session.add(new_sub_cps_m_agg)
            db.session.commit()
            db.session.close()
        except IntegrityError:
            db.session.rollback()
            db.session.close()
    return jsonify(summary), 200