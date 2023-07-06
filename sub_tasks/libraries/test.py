from airflow.models import Variable
import pandas as pd
from sub_tasks.libraries.utils import (path)
lateness_path = pd.ExcelFile(
            "{path}opening_time.xlsx".format(path=path)).sheet_names

# lateness_report = lateness_path.parse(sheet_name="summary")
print(lateness_path)
