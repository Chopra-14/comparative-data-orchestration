from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

# ---------------- ETL LOGIC ---------------- #

def extract_data(**context):
    df = pd.read_csv(
        "/opt/airflow/data/synthetic_events.csv",
        parse_dates=["timestamp"]
    )
    context["ti"].xcom_push(key="raw_df", value=df.to_json(date_format="iso"))

def transform_data(**context):
    df = pd.read_json(context["ti"].xcom_pull(key="raw_df"))

    # remove blocked countries (same as Prefect/Dagster)
    df = df[~df["country"].isin(["US", "CN"])]

    # detect sessions
    session = (
        df.groupby("user_id")["timestamp"]
        .agg(["min", "max"])
        .reset_index()
    )

    session["session_duration_seconds"] = (
        session["max"] - session["min"]
    ).dt.total_seconds()

    df["date"] = df["timestamp"].dt.date

    agg = (
        df.groupby(["user_id", "date"])
        .size()
        .reset_index(name="event_count")
    )

    final = agg.merge(
        session[["user_id", "session_duration_seconds"]],
        on="user_id",
        how="left"
    )

    context["ti"].xcom_push(
        key="final_df",
        value=final.to_json(date_format="iso")
    )

def load_data(**context):
    df = pd.read_json(context["ti"].xcom_pull(key="final_df"))

    output_path = "/opt/airflow/data/output_airflow"
    Path(output_path).mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(df)

    pq.write_to_dataset(
        table,
        root_path=output_path,
        partition_cols=["date"]
    )

# ---------------- DAG ---------------- #

with DAG(
    dag_id="etl_airflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_data,
    )

    extract >> transform >> load
