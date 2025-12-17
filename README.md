# Comparative Data Orchestration

### Airflow vs Prefect vs Dagster

##  Project Overview

This project demonstrates how the **same ETL pipeline** can be implemented and executed using **three different data orchestration tools**:

* **Apache Airflow**
* **Prefect**
* **Dagster**

The main goal is to compare these tools while ensuring that:

* The **business logic is identical**
* The **output data is consistent**
* The **schema and partitions match**

Each orchestrator runs the same ETL process on the same input dataset and produces partitioned Parquet outputs.

---

##  Tools & Technologies Used

* **Python 3**
* **Apache Airflow (Docker-based)**
* **Prefect**
* **Dagster**
* **Pandas**
* **PyArrow / Parquet**
* **Docker & Docker Compose**

---

##  Input Data

* `synthetic_events.csv`
* Contains:

  * `user_id`
  * `timestamp`
  * `country`

The dataset is processed daily and partitioned by `date`.

---

##  ETL Pipeline Logic (Same for all tools)

1. **Extract**

   * Read `synthetic_events.csv`
   * Parse timestamps

2. **Transform**

   * Remove blocked countries (`US`, `CN`)
   * Calculate:

     * `event_count` per user per day
     * `session_duration_seconds` per user
   * Add `date` column from timestamp

3. **Load**

   * Write output as **Parquet**
   * Partitioned by `date`

---

##  How to Run Each Pipeline

---

###  Airflow

#### Start Airflow

```bash
cd airflow
docker compose up -d
```

#### Open UI

```
http://localhost:8080
```

#### Trigger DAG

* DAG name: `etl_airflow`
* Turn ON the DAG
* Click ▶ (Trigger DAG)

#### Output location

```bash
airflow/data/output_airflow/
```

---

###  Prefect

#### Run Prefect pipeline

```bash
python etl/prefect_flow.py
```

#### Output location

```bash
output_prefect/
```

---

###  Dagster

#### Run Dagster job

```bash
python dagster/job.py
```

#### Output location

```bash
output_dagster/
```

---

##  Backfill Commands

Backfill is used to process historical dates.

---

### Airflow Backfill

```bash
airflow dags backfill etl_airflow \
  --start-date 2024-01-01 \
  --end-date 2024-01-02
```

---

### Prefect Backfill

```bash
python etl/prefect_flow.py
```

*(Dates are handled inside the script)*

---

### Dagster Backfill

```bash
python dagster/job.py
```

*(Partitions handled programmatically)*

---

##  Output Parity Verification

The outputs from **Airflow**, **Prefect**, and **Dagster** were verified.

###  Schema (identical across all tools)

* `user_id` → int64
* `event_count` → int64
* `session_duration_seconds` → double
* `date` → date

###  Partitioning

* `date=YYYY-MM-DD`

###  Business Logic

* Same filters
* Same aggregations
* Same session calculation

Minor differences like file ordering or folder naming are due to internal execution behavior and **do not affect correctness**.

---

##  Conclusion

This project shows that different orchestration tools can be used to run the **same data pipeline** with **consistent outputs**.

* Airflow is more traditional and scheduler-driven
* Prefect is lightweight and developer-friendly
* Dagster provides strong structure and data awareness

All three successfully achieve the same result.
