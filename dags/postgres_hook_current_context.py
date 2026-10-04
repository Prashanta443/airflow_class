import pendulum
import requests

from airflow.sdk import dag, task,get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from ingestion import (
    extract_from_api,
    create_table_if_not_exists,
    load_to_postgres,
    validate_load

)



@dag(
    dag_id="api_ingest_using_current_context_data",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 28, tz="UTC"),
    catchup=False,
    tags=["api", "postgres", "etl", "xcom"],
)
def api_to_postgres_photos():

    extract = extract_from_api()
    create_table = create_table_if_not_exists()
    load = load_to_postgres()
    validate = validate_load()


    [extract, create_table] >> load >> validate


api_to_postgres_photos()