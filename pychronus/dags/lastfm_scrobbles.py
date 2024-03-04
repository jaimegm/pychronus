from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from pychronus.operators.lastfm import LastFmOperator

default_args = {
    "owner": "Jaime",
    "start_date": datetime(2022, 2, 20),
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}


dag = DAG(
    "lastfm_scrobbles",
    default_args=default_args,
    schedule_interval="* * * * *",
    catchup=False,
    max_active_runs=1,
)


with dag:

    BashOperator(
        task_id="force_trigger",
        bash_command="echo 1",
    )

    extract_lastfm = LastFmOperator(
        task_id="extract_lastfm",
        database="pythos",
        updated_at="timestamp",
        username="JaiMAliin",
        method="user.getrecenttracks",
    )
