from app import create_app
from app.scheduler import job_cps_d_agg, job_cps_sub_m_agg, job_cps_m_agg, job_cps_w_agg, job_cps_sub_w_agg

app = create_app()
# job_cps_sub_m_agg(app)
# job_cps_m_agg(app)
# job_cps_d_agg(app)
job_cps_sub_w_agg(app)
##job_cps_w_agg(app)
if __name__ == '__main__':
    app.run(debug=True)
