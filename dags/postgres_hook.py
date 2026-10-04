import pendulum
import requests

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_URL = "https://jsonplaceholder.typicode.com/photos"
DEST_CONN_ID = "dest_postgres"
TARGET_TABLE = "photos"


@dag(
    dag_id="api_to_postgres_ingest_data",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 28, tz="Asia/Kathmandu"),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=1),
    },
    tags=["api", "postgres", "etl"],
)
def api_to_postgres_photos():

    @task
    def extract_from_api() -> list[dict]:

        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()

        photos = response.json()
        print(f"Fetched {len(photos)} records from API")
        return photos

   
    @task
    def create_table_if_not_exists():
        """Creates the photos table in destination Postgres if it doesn't exist."""
        hook = PostgresHook(postgres_conn_id=DEST_CONN_ID)

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                photo_id      INT PRIMARY KEY,
                album_id      INT           NOT NULL,
                title         TEXT          NOT NULL,
                url           TEXT          NOT NULL,
                thumbnail_url TEXT          NOT NULL,
                loaded_at     TIMESTAMP     DEFAULT NOW()
            );
        """
        hook.run(create_sql)
        print(f"Table '{TARGET_TABLE}' is ready.")


    @task
    def load_to_postgres(photos: list[dict]) -> dict:
        """
        Upserts all photo records into destination Postgres.
        Uses INSERT ... ON CONFLICT DO UPDATE so re-runs are safe.
        Returns a summary dict with the row count.
        """
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

        # Build rows as tuples matching the SQL placeholders
        rows = [
            (
                photo["id"],
                photo["albumId"],
                photo["title"],
                photo["url"],
                photo["thumbnailUrl"],
            )
            for photo in photos
        ]

        conn = hook.get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.executemany(upsert_sql, rows)
            conn.commit()
        finally:
            conn.close()

        print(f"Upserted {len(rows)} rows into '{TARGET_TABLE}'")
        return {"rows_loaded": len(rows), "table": TARGET_TABLE}

    @task
    def validate_load(load_result: dict):
        """Sanity check: the table must contain at least the rows we just loaded."""
        hook = PostgresHook(postgres_conn_id=DEST_CONN_ID)

        count = hook.get_first(f"SELECT COUNT(*) FROM {TARGET_TABLE};")[0]
        expected = load_result["rows_loaded"]

        print(f"Rows in DB: {count} | Rows loaded this run: {expected}")

        if count < expected:
            raise ValueError(
                f"Validation failed: {count} rows in DB, expected at least {expected}"
            )

        print("Validation passed")



    photos_data = extract_from_api()
    table_ready = create_table_if_not_exists()
    load_result = load_to_postgres(photos_data)   
    validate = validate_load(load_result)         


    table_ready >> load_result


api_to_postgres_photos()