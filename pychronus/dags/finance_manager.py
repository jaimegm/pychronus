from datetime import datetime

from airflow import DAG

from pychronus.operators.finance_manager import FinanceManagerOperator

dag = DAG(
    "finance_manager",
    schedule_interval=None,
    max_active_runs=1,
    default_args={
        "start_date": datetime(2023, 2, 13),
        "depends_on_past": False,
    },
    catchup=False,
)

with dag:
    process_transactions = FinanceManagerOperator(task_id="Process_Transactions")
    process_transactions
