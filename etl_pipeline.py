"""
Simple example of an ETL pipeline in Python.

- Extract: read data from a CSV file
- Transform: clean column names and filter rows
- Load: write cleaned data into a SQLite database table

Note:
The file paths below are examples. In a real project you would
update them to match where your CSV and database live.
"""

import pandas as pd
import sqlite3
from pathlib import Path


def extract(csv_path: str) -> pd.DataFrame:
    """Extract data from a CSV file into a Pandas DataFrame."""
    print(f"Extracting data from {csv_path}...")
    df = pd.read_csv(csv_path)
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the data:
    - standardize column names
    - drop rows with missing values
    - filter to recent records (example)
    """
    print("Transforming data...")

    # clean column names
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # example: drop any rows with nulls
    df = df.dropna()

    # if there's a date column, you could filter by date (optional)
    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
        df = df[df["order_date"] >= "2024-01-01"]

    return df


def load(df: pd.DataFrame, db_path: str, table_name: str) -> None:
    """Load the DataFrame into a SQLite database table."""
    print(f"Loading data into {db_path} (table: {table_name})...")
    conn = sqlite3.connect(db_path)
    try:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    finally:
        conn.close()
    print("Load complete.")


if __name__ == "__main__":
    # Example paths for a local run (can be adjusted as needed)
    input_csv = "sample_data.csv"      # e.g. a sales or orders CSV
    output_db = "analytics.db"         # SQLite database file
    table = "clean_sales_data"

    # Make sure the CSV exists before running in a real environment
    if not Path(input_csv).exists():
        print(f"WARNING: {input_csv} does not exist. "
              "This script is for portfolio/demo purposes.")
    else:
        raw_df = extract(input_csv)
        clean_df = transform(raw_df)
        load(clean_df, output_db, table)

        print("ETL pipeline completed successfully.")
