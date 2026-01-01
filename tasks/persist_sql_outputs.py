from pathlib import Path
import pandas as pd
from prefect import task
from tasks.sql_executor import execute_sql_file

@task
def persist_sql_outputs(sql_file_path: str, output_dir: str) -> list[str]:
    """
    Executes a SQL file and writes each SELECT result set to a CSV.
    Returns the list of generated CSV file paths.
    """
    Path(output_dir).mkdir(exist_ok=True, parents=True)

    results = execute_sql_file.fn(sql_file_path, return_results=True)
    csv_paths = []

    for i, df in enumerate(results, start=1):
        csv_file = Path(output_dir) / f"{Path(sql_file_path).stem}_result_{i}.csv"
        df.to_csv(csv_file, index=False)
        csv_paths.append(str(csv_file))
        print(f"[PERSIST SQL] Saved {len(df)} rows to {csv_file}")

    return csv_paths
