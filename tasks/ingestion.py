# tasks/ingestion.py

from prefect import task
import pandas as pd

@task
def ingest_csv(file_path: str, expected_columns: list[str]):
    """
    Reads a CSV file, validates schema shape, logs row counts,
    and returns a cleaned pandas DataFrame.
    """
    df = pd.read_csv(file_path)

    row_count = len(df)
    print(f"[INGESTION] Loaded {row_count} rows from {file_path}")

    # Validate missing / extra columns
    missing_cols = set(expected_columns) - set(df.columns)
    extra_cols = set(df.columns) - set(expected_columns)

    if missing_cols:
        raise ValueError(
            f"[INGESTION ERROR] Missing required columns: {missing_cols}"
        )

    if extra_cols:
        print(
            f"[INGESTION WARNING] Extra columns ignored: {extra_cols}"
        )
        df = df[list(expected_columns)]

    print(f"[INGESTION] Schema validated for {file_path}")

    return df
