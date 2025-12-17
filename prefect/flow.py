import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from prefect import flow, task
from etl_logic.pipeline import extract_data, transform_data, load_data

@task(retries=2, retry_delay_seconds=10)
def transform(df):
    return transform_data(df, ["US", "CN"])

@flow
def etl_flow(input_path: str, output_path: str):
    df = extract_data(input_path)
    transformed = transform(df)
    load_data(transformed, output_path)

if __name__ == "__main__":
    etl_flow(
        input_path="synthetic_events.csv",
        output_path="output_prefect"
    )
