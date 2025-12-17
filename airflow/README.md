### Apache Airflow – ETL Pipeline

This folder contains the Apache Airflow implementation of the ETL pipeline used in the Comparative Data Orchestration project.

The goal of this pipeline is to demonstrate how the same ETL logic can be orchestrated using Airflow and compared with other tools like Prefect and Dagster.

📌 Overview

The Airflow pipeline performs the following steps:

Extract

Reads event data from a CSV file (synthetic_events.csv)

Transform

Filters out blocked countries

Computes:

Event count per user per day

Session duration per user

Load

Writes the final result as Parquet files

Data is partitioned by date

Each step is implemented as a separate Airflow task.

📁 Folder Structure
airflow/
│
├── dags/
│   └── etl_airflow.py
│
├── data/
│   ├── synthetic_events.csv
│   └── output_airflow/
│       └── date=YYYY-MM-DD/
│
├── docker-compose.yml
└── README.md

⚙️ Technologies Used

Apache Airflow 2.8.1

Docker & Docker Compose

PostgreSQL (Airflow metadata database)

Pandas

PyArrow (Parquet output)

▶️ How to Run the Airflow Pipeline
1️⃣ Start Airflow Services

From the airflow/ directory:

docker compose up -d


This starts:

PostgreSQL

Airflow Webserver

Airflow Scheduler

2️⃣ Access Airflow UI

Open your browser and go to:

http://localhost:8080


Login credentials (if required):

Username: admin
Password: admin

3️⃣ Enable the DAG

Locate the DAG named etl_airflow

Turn the toggle ON

Trigger the DAG manually or let it run on schedule

🔁 Backfill Execution

Airflow supports running pipelines for historical dates.

Example backfill command:

airflow dags backfill etl_airflow -s 2024-01-01 -e 2024-01-03


This executes the pipeline for past dates and generates corresponding partitions.

📤 Output

The output is written to:

airflow/data/output_airflow/


Partitioned by date:

output_airflow/
├── date=2024-01-01/
├── date=2024-01-02/
└── date=2024-01-03/


Each partition contains Parquet files with identical schema to the Prefect and Dagster outputs.

🧠 Key Learnings

Airflow requires explicit DAG and task definitions

Scheduler and webserver must both be running

Backfill is a strong feature for historical reprocessing

Setup is more complex compared to Prefect, but very powerful for production workflows

✅ Status

✔ Pipeline runs successfully
✔ Output generated in Parquet format
✔ Used for comparison with Prefect and Dagster