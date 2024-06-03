from airflow.models import variable
import pandas as pd


def create_insurance_submission(data, path) -> None:
    with pd.ExcelWriter(path) as writer:
        data.to_excel(writer, index = False)
