from pathlib import Path
from prefect import task
from tasks.db import get_db_connection


@task
def execute_sql_file(sql_file_path: str):
    """
    Executes a SQL file containing one or more statements.
    Assumes the file controls transaction boundaries
    (START TRANSACTION / COMMIT).
    """

    sql_path = Path(sql_file_path)

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    with open(sql_path, "r", encoding="utf-8") as f:
        sql_script = f.read()

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Split statements on semicolon
        statements = [
            stmt.strip()
            for stmt in sql_script.split(";")
            if stmt.strip()
        ]

        for statement in statements:
            cursor.execute(statement)

        # Commit only if all statements succeed
        conn.commit()
        print(f"[SQL EXECUTOR] Successfully executed {sql_path.name}")

    except Exception as e:
        conn.rollback()
        raise RuntimeError(
            f"[SQL EXECUTOR ERROR] Failed executing {sql_path.name}: {e}"
        )

    finally:
        cursor.close()
        conn.close()
