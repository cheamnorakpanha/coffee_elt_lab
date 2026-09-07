from datetime import datetime

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from google.cloud import bigquery


def extract_and_load():
    from google.oauth2 import service_account  # type: ignore

    key_path = "/usr/local/airflow/include/gcp_key.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path)

    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )

    files = {
        "customers": "customers_dirty.csv",
        "orders": "orders_dirty.csv",
        "products": "products_dirty.csv",
        "promotions": "promotions_dirty.csv",
    }

    for table_name, file_name in files.items():
        file_path = f"/usr/local/airflow/include/{file_name}"

        with open(file_path, "rb") as source_file:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=False,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )

            with open(file_path, "r", encoding="utf-8") as csv_file:
                header = csv_file.readline().strip().split(",")

            job_config.schema = [
                bigquery.SchemaField(column, "STRING") for column in header
            ]

            table_id = f"{credentials.project_id}.raw.{table_name}"

            load_job = client.load_table_from_file(
                source_file,
                table_id,
                job_config=job_config,
            )

            load_job.result()

            print(f"Loaded {file_name} into {table_id}")


def transform():
    from google.oauth2 import service_account  # type: ignore

    key_path = "/usr/local/airflow/include/gcp_key.json"

    credentials = service_account.Credentials.from_service_account_file(
        key_path)

    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )

    query = f"""
    CREATE OR REPLACE TABLE `{credentials.project_id}.analytics.orders_enriched` AS

    WITH customers_clean AS (
        SELECT
            TRIM(customer_id) AS customer_id,
            CONCAT(
                INITCAP(TRIM(first_name)),
                ' ',
                INITCAP(TRIM(last_name))
            ) AS customer_name
        FROM `{credentials.project_id}.raw.customers`
        WHERE NULLIF(TRIM(customer_id), '') IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TRIM(customer_id)
            ORDER BY customer_id
        ) = 1
    ),

    orders_clean AS (
        SELECT
            TRIM(order_id) AS order_id,
            TRIM(customer_id) AS customer_id,

            COALESCE(
                SAFE.PARSE_DATE('%Y-%m-%d', TRIM(order_date)),
                SAFE.PARSE_DATE('%m/%d/%Y', TRIM(order_date)),
                SAFE.PARSE_DATE('%d-%b-%Y', TRIM(order_date))
            ) AS order_date_clean,

            CASE
                WHEN LOWER(TRIM(status)) IN ('completed', 'complete')
                    THEN 'Completed'
                WHEN LOWER(TRIM(status)) = 'pending'
                    THEN 'Pending'
                WHEN LOWER(TRIM(status)) IN ('cancelled', 'canceled')
                    THEN 'Cancelled'
                WHEN LOWER(TRIM(status)) = 'refunded'
                    THEN 'Refunded'
                WHEN LOWER(TRIM(status)) = 'shipped'
                    THEN 'Shipped'
                WHEN LOWER(TRIM(status)) = 'in progress'
                    THEN 'In Progress'
                ELSE 'Unknown'
            END AS status_clean,

            NULLIF(UPPER(TRIM(promo_code)), '') AS promo_code_clean,

            total_amount

        FROM `{credentials.project_id}.raw.orders`
    ),

    promotions_clean AS (
        SELECT
            UPPER(TRIM(promo_code)) AS promo_code_clean,
            LOWER(TRIM(channel)) AS promo_channel,

            CASE
                WHEN SAFE_CAST(
                    REPLACE(TRIM(discount_pct), '%', '')
                    AS FLOAT64
                ) < 0
                    THEN NULL

                WHEN SAFE_CAST(
                    REPLACE(TRIM(discount_pct), '%', '')
                    AS FLOAT64
                ) < 1
                    THEN SAFE_CAST(
                        REPLACE(TRIM(discount_pct), '%', '')
                        AS FLOAT64
                    ) * 100

                ELSE SAFE_CAST(
                    REPLACE(TRIM(discount_pct), '%', '')
                    AS FLOAT64
                )
            END AS discount_pct

        FROM `{credentials.project_id}.raw.promotions`
        WHERE NULLIF(TRIM(promo_code), '') IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(promo_code))
            ORDER BY promo_code
        ) = 1
    )

    SELECT
        o.order_id,
        o.customer_id,
        c.customer_name,
        o.order_date_clean,
        o.status_clean,
        o.promo_code_clean,
        p.discount_pct,
        p.promo_channel,
        o.total_amount

    FROM orders_clean o

    LEFT JOIN customers_clean c
        ON o.customer_id = c.customer_id

    LEFT JOIN promotions_clean p
        ON o.promo_code_clean = p.promo_code_clean
    """

    job = client.query(query)
    job.result()

    print(f"Created {credentials.project_id}.analytics.orders_enriched")


with DAG(
    dag_id="coffee_elt_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract_and_load_task = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    extract_and_load_task >> transform_task
