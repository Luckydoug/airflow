from airflow.models import variable
from sub_tasks.oho_yor_targets.reports.targets import (create_incentive)
from sub_tasks.oho_yor_targets.fetch_data import (
    fetch_orders,
    fetch_targets,
    fetch_payments,
    fetch_journals,
    fetch_cash_payments,
)

orders = fetch_orders()
targets = fetch_targets()
all_journals = fetch_journals()
all_payments = fetch_payments()
cash_payments = fetch_cash_payments()


def build_incentives():
    create_incentive(
        all_payments, 
        all_journals, 
        orders, 
        cash_payments, 
        targets
        )
