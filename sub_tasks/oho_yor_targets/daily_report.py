from airflow.models import variable
from sub_tasks.oho_yor_targets.reports.targets import (create_incentive)
from sub_tasks.oho_yor_targets.fetch_data import (
    fetch_targets,
    fetch_cash_payments,
    fetch_insurance_payments,
)


def targets():
    targets = fetch_targets()

    return targets


def cash_payments():
    cash_payments = fetch_cash_payments()

    return cash_payments


def insurance_payments():
    insurance_payments = fetch_insurance_payments()

    return insurance_payments


def build_incentives():
    create_incentive(
        cash_payments=cash_payments(),
        targets=targets(),
        insurance_payments=insurance_payments(),
    )
