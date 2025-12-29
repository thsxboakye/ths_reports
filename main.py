from reports.diarrhea_report import run_diarrhea_report
from reports.kitten_report import run_kitten_report
from reports.parvo_report import run_parvo_report
from reports.incidence_report import run_incidence_report
from reports.dental_report import run_dental_report
from reports.ringworm_report import run_ringworm_report
from reports.uri_report import run_uri_report
from reports.sx_wait_time import run_sx_wait_time_report
from reports.delayed_euthanasia import run_euthanasia_report
from reports.los_shelter_report import run_los_report
from reports.ezyvet import get_ezyvet_report
from datetime import datetime

report_year= datetime.today().year
report_month=datetime.today().month
def run_all():
    run_diarrhea_report(report_year)
    run_kitten_report(report_year)
    run_parvo_report(report_year)
    run_dental_report(report_year)
    run_incidence_report(report_year)
    run_ringworm_report(report_year)
    run_uri_report(report_year)
    run_los_report(report_year)
    run_sx_wait_time_report(report_year)
    run_euthanasia_report(report_month, report_year)
    return 

run_all()

