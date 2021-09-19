import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

from pychronus.hooks.gsheet import GSheetHook

default_args = {
    "owner": "Jaime",
    "start_date": datetime(2020, 2, 22),
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}


dag = DAG(
    "crypto_sheets",
    default_args=default_args,
    schedule_interval="*/5 * * * 1-5",
    catchup=False,
    max_active_runs=1,
)


gconfig = {
    "sheetid": "1O5z9l_u26oYrVWpdxILqE4Pc3mCsYuFgnNGp9KdMULw",
    "range": "Command Test!A1:F",
}


# Check Google doc for updates
def check_doc(sheet_id, cell):
    logging.info("pulling config options")
    gsheet = GSheetHook()
    command = gsheet.get_values_df(sheet_id, cell)
    command = str(command.loc[0]["command"])
    # if command = Run Then Run Pipeline
    if command.upper() == "RUN":
        logging.info("Triggering Pipeline")
        return True
    else:
        logging.info("No Items to update")
        return False


def make_order():
    gsheet = GSheetHook()
    df = pd.DataFrame(
        {
            "command": ["Items have be updated"],
            "last_run": [str(datetime.now())],
            "last state": ["Pushed Items"],
        }
    )
    # Update Google Doc
    gsheet.write_values(
        "1zd71mM1UKsNlTQBBW7qeqJ-1oPKg_StjB70N-kcBVD8", "DRP_List", df, "E1"
    )
    logging.info("Updated Google doc with latest pipeline status")


with dag:

    check_status_sheet = ShortCircuitOperator(
        task_id="Check_Status_Sheet",
        python_callable=check_doc,
        op_kwargs={"sheet_id": gconfig["sheetid"], "cell": gconfig["range"]},
    )

    make_move = PythonOperator(
        task_id="Update_Items",
        python_callable=make_order,
    )

    check_status_sheet >> make_move
