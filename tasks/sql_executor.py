# sql_executor.py
from pathlib import Path
from prefect import task
import pandas as pd
from tasks.db import get_db_connection


@task
def execute_sql_file(
    sql_file_path: str,
    return_results: bool = False
) -> list[pd.DataFrame] | None:
    """
    Executes a SQL file containing one or more statements.

    Args:
        sql_file_path: Path to SQL file
        return_results: If True, captures SELECT result sets as pandas DataFrames

    Returns:
        List of DataFrames (in execution order) if return_results=True, else None
    """

    sql_path = Path(sql_file_path)
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    with open(sql_path, "r", encoding="utf-8") as f:
        sql_script = f.read()

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    results: list[pd.DataFrame] = []
    has_modifications = False

    try:
        statements = [s.strip() for s in sql_script.split(";") if s.strip()]

        for statement in statements:
            cursor.execute(statement)

            if cursor.with_rows:
                # 🔑 ALWAYS consume rows
                rows = cursor.fetchall()

                # Only store if requested
                if return_results:
                    results.append(pd.DataFrame(rows))
            else:
                has_modifications = True

        if has_modifications:
            conn.commit()

        print(f"[SQL EXECUTOR] Successfully executed {sql_path.name}")
        return results if return_results else None

    except Exception as e:
        conn.rollback()
        raise RuntimeError(
            f"[SQL EXECUTOR ERROR] Failed executing {sql_path.name}: {e}"
        )

    finally:
        cursor.close()
        conn.close()
