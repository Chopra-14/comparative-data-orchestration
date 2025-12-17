from etl_logic.pipeline import extract_data, transform_data, load_data

INPUT_PATH = "synthetic_events.csv"
OUTPUT_PATH = "output_test"

BLOCKED_COUNTRIES = ["US", "CN"]

print("STEP 1: Extracting data...")
df = extract_data(INPUT_PATH)
print(df.head())

print("STEP 2: Transforming data...")
df_transformed = transform_data(df, BLOCKED_COUNTRIES)
print(df_transformed.head())

print("STEP 3: Loading data to Parquet...")
load_data(df_transformed, OUTPUT_PATH)

print("✅ ETL Pipeline completed successfully")
