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


def activate(orgs):
    gsheet = GSheetHook()
    # drp_line_items = gsheet.get_values_df(
    #    "1zd71mM1UKsNlTQBBW7qeqJ-1oPKg_StjB70N-kcBVD8", "DRP_List!A1:C"
    # )
    # Loop through item updates
    update_status = list()
    last_completed_run = datetime.now()
    update_ui = {
        "command": ["Items have be updated"],
        "last_run": [str(last_completed_run)],
        "last state": ["Pushed Items"],
    }
    df = pd.DataFrame(update_ui)
    logs = pd.DataFrame(update_status, columns=["Status", "log"])
    # Update Google Doc
    gsheet.write_values(
        "1zd71mM1UKsNlTQBBW7qeqJ-1oPKg_StjB70N-kcBVD8", "DRP_List", df, "E1"
    )
    gsheet.write_values(
        "1zd71mM1UKsNlTQBBW7qeqJ-1oPKg_StjB70N-kcBVD8", "DRP_List", logs, "E1"
    )
    logging.info("Updated Google doc with latest pipeline status")


with dag:
    check_status_sheet = ShortCircuitOperator(
        task_id="Check_Status_Sheet",
        python_callable=check_doc,
        op_kwargs={
            "sheet_id": "1zd71mM1UKsNlTQBBW7qeqJ-1oPKg_StjB70N-kcBVD8",
            "cell": "Items!E1:E2",
        },
    )

    change_status = PythonOperator(
        task_id="Update_Items",
        python_callable=activate,
        op_kwargs={"orgs": "92164512"},
    )

    check_status_sheet >> change_status
