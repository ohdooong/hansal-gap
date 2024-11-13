from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    # "on_failure_callback": ,
}

dag = DAG("download-github-archive", 
          default_args=default_args, 
          max_active_runs=1, 
          schedule_interval="0 * * * *", 
          catchup=False, 
          tags=['data'])

dt = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
download_data = BashOperator(
    task_id='download-data',
    bash_command=f"/opt/airflow/jobs/download-data.sh {dt} ",
    dag=dag
)
