
from dagster import op, job, RetryPolicy
from etl_logic.pipeline import extract_data, transform_data, load_data

@op
def extract():
    return extract_data("synthetic_events.csv")

@op(retry_policy=RetryPolicy(max_retries=2, delay=10))
def transform(df):
    return transform_data(df, ["US", "CN"])

@op
def load(df):
    load_data(df, "output_dagster")

@job
def etl_job():
    load(transform(extract()))
