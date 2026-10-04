from datetime import timedelta
from pendulum import datetime

from airflow.sdk import dag,task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

@dag(
    dag_id = 'master_dag_run',
    schedule = '30 2 * * *',
    catchup = False,
    is_paused_upon_creation=False,
    start_date=datetime(2026, 1, 1,tz="Asia/Kathmandu"),
    tags = ["master dag","orchestrate different dag",'scheduling']


)
def main_dag():

    start = EmptyOperator(
        task_id="start"
    )

    trigger_ingestion_and_load = TriggerDagRunOperator(
        task_id = "ingesting_and_load",
        trigger_dag_id = "api_ingest_using_current_context_data",
        wait_for_completion = True
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> trigger_ingestion_and_load >> end

main_dag()
