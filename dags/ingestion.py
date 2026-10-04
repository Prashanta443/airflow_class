import pendulum
import requests

from airflow.sdk import dag, task,get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from crediantials import API_URL,DEST_CONN_ID,TARGET_TABLE



@task
def extract_from_api(**context):
    # context = get_current_context()
    # response = get_api _response(API_URL)
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    photos = response.json()
    print(f"Fetched {len(photos)} records from API")
    context["ti"].xcom_push(key="photos_data", value=photos)
    context["ti"].xcom_push(key="record_count", value=len(photos))







@task
def create_table_if_not_exists(**context):
    
    print(f"Running for logical date: {context['ds']}")
    hook = PostgresHook(postgres_conn_id=DEST_CONN_ID)
    hook.run(
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            photo_id      INT PRIMARY KEY,
            album_id      INT       NOT NULL,
            title         TEXT      NOT NULL,
            url           TEXT      NOT NULL,
            thumbnail_url TEXT      NOT NULL,
            loaded_at     TIMESTAMP DEFAULT NOW()
        );
        """
    )
    print(f"Table '{TARGET_TABLE}' is ready.")







@task
def load_to_postgres(**context):
    photos = context["ti"].xcom_pull(task_ids="extract_from_api", key="photos_data")

    if not photos:
        raise ValueError("No photos found in XCom. Did extract_from_api run?")

    hook = PostgresHook(postgres_conn_id=DEST_CONN_ID)
    
    upsert_sql = f"""
        INSERT INTO {TARGET_TABLE} (photo_id, album_id, title, url, thumbnail_url)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (photo_id)
        DO UPDATE SET
            album_id      = EXCLUDED.album_id,
            title         = EXCLUDED.title,
            url           = EXCLUDED.url,
            thumbnail_url = EXCLUDED.thumbnail_url,
            loaded_at     = NOW();
    """
    rows = [
        (p["id"], p["albumId"], p["title"], p["url"], p["thumbnailUrl"])
        for p in photos
    ]
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.executemany(upsert_sql, rows)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Upserted {len(rows)} rows into '{TARGET_TABLE}'")
    context["ti"].xcom_push(key="rows_loaded", value=len(rows))






@task
def validate_load(**context):
    """PULL values from two different tasks and compare them."""
    ti = context["ti"]
    rows_loaded = ti.xcom_pull(task_ids="load_to_postgres", key="rows_loaded")
    records_fetched = ti.xcom_pull(task_ids="extract_from_api", key="record_count")
    hook = PostgresHook(postgres_conn_id=DEST_CONN_ID)
    db_count = hook.get_first(f"SELECT COUNT(*) FROM {TARGET_TABLE};")[0]
    print(f"Fetched from API : {records_fetched}")
    print(f"Loaded this run  : {rows_loaded}")
    print(f"Rows in database : {db_count}")
    if rows_loaded != records_fetched:
        raise ValueError("Mismatch: rows loaded != records fetched!")
    if db_count < rows_loaded:
        raise ValueError("Validation failed: DB has fewer rows than were loaded!")
    print("Validation passed")