import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# ---------- EXTRACT ----------
def extract_data(input_path: str) -> pd.DataFrame:
    df = pd.read_csv(input_path, parse_dates=["timestamp"])
    print(f"Extracted {len(df)} rows")
    return df

# ---------- TRANSFORM ----------
def transform_data(df: pd.DataFrame, blocked_countries: list) -> pd.DataFrame:
    df = df[~df["country"].isin(blocked_countries)]

    df["date"] = df["timestamp"].dt.date

    session = (
        df.groupby("user_id")["timestamp"]
        .agg(["min", "max"])
        .reset_index()
    )

    session["session_duration_seconds"] = (
        session["max"] - session["min"]
    ).dt.total_seconds()

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

    return final

# ---------- LOAD ----------
def load_data(df: pd.DataFrame, output_path: str):
    Path(output_path).mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(df)

    pq.write_to_dataset(
        table,
        root_path=output_path,
        partition_cols=["date"]
    )
