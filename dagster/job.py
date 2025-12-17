# import sys
# import os

# # 🔧 Fix import path so etl_logic is found
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# from dagster import op, job, RetryPolicy
# from etl_logic.pipeline import extract_data, transform_data, load_data


# @op
# def extract():
#     return extract_data("synthetic_events.csv")


# @op(
#     retry_policy=RetryPolicy(
#         max_retries=2,
#         delay=10
#     )
# )
# def transform(df):
#     return transform_data(df, ["US", "CN"])


# @op
# def load(df):
#     load_data(df, "output_dagster")


# @job
# def etl_job():
#     load(transform(extract()))

# # import os
# # from dagster import op, job, RetryPolicy
# # from etl_logic.pipeline import extract_data, transform_data, load_data

# # BASE_DIR = os.path.dirname(os.path.dirname(__file__))

# # @op
# # def extract():
# #     return extract_data(os.path.join(BASE_DIR, "synthetic_events.csv"))

# # @op(retry_policy=RetryPolicy(max_retries=2, delay=10))
# # def transform(df):
# #     return transform_data(df, ["US", "CN"])

# # @op
# # def load(df):
# #     load_data(df, os.path.join(BASE_DIR, "output_dagster"))

# # @job
# # def etl_job():
# #     load(transform(extract()))

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
