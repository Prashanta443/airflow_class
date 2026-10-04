from datetime import timedelta
from pendulum import datetime

from airflow.sdk import dag,task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


def say_hello():
    print("Hello from PythonOperator!")


@dag(
    dag_id="schedule_interval",
    start_date=datetime(2026, 1, 1,tz="Asia/Kathmandu"),
    schedule="30 2 * * *",
    is_paused_upon_creation=False,  
    catchup=False,
    max_active_runs=1,
    tags=['scheduling dag','learn scheduling']
)

def schedule_example():
    @task.python(
        retries=3,
        retry_delay=timedelta(minutes=5)
    )
    def process_data():
        print("Processing data...")

    start = EmptyOperator(
        task_id="start"
    )
    @task.bash
    def bash_task():
        return "echo 'Hello from BashOperator!'"



    end = EmptyOperator(
        task_id="end-task"
    )
    # there should not be sapce in between
    bash_task = bash_task()
    process_data = process_data()
    start >> bash_task >> process_data >> end

schedule_example()