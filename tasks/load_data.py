from pathlib import Path
import pandas as pd
from prefect import task

from tasks.db import get_db_connection


# Base directory resolution (repo-safe, machine-safe)
DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"


# Explicit load order to satisfy foreign keys
TABLE_LOAD_CONFIG = [
    {
        "table": "customers",
        "columns": [
            "customer_id", "name", "customer_type",
            "email", "phone", "address"
        ]
    },
    {
        "table": "suppliers",
        "columns": [
            "supplier_id", "supplier_name", "material_type",
            "contact_email", "country"
        ]
    },
    {
        "table": "raw_materials",
        "columns": [
            "material_id", "material_name", "unit_of_measure",
            "supplier_id", "cost_per_unit"
        ]
    },
    {
        "table": "drugs",
        "columns": [
            "drug_id", "drug_name", "dosage_form",
            "strength_mg", "price"
        ]
    },
    {
        "table": "drug_formulations",
        "columns": [
            "formulation_id", "drug_id",
            "material_id", "quantity_required"
        ]
    },
    {
        "table": "drug_batches",
        "columns": [
            "batch_id", "drug_id", "batch_number",
            "manufacture_date", "expiration_date",
            "quantity_produced"
        ]
    },
    {
        "table": "orders",
        "columns": [
            "order_id", "customer_id", "order_date"
        ]
    },
    {
        "table": "order_items",
        "columns": [
            "order_item_id", "order_id", "batch_id",
            "quantity", "unit_price"
        ]
    }
]


def _build_insert_sql(table: str, columns: list[str]) -> str:
    placeholders = ", ".join(["%s"] * len(columns))
    column_list = ", ".join(columns)

    return f"""
        INSERT INTO {table} ({column_list})
        VALUES ({placeholders})
    """


@task(retries=2, retry_delay_seconds=10)
def load_all_csv_data(batch_size: int = 1000):
    """
    Loads all CSV files into MySQL using parameterized batch inserts.
    Entire load runs inside a single transaction.
    """

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        for config in TABLE_LOAD_CONFIG:
            table = config["table"]
            columns = config["columns"]

            csv_path = DATA_DIR / f"{table}.csv"
            if not csv_path.exists():
                raise FileNotFoundError(f"Missing CSV file: {csv_path}")

            df = pd.read_csv(csv_path)

            # Enforce column order explicitly
            df = df[columns]

            insert_sql = _build_insert_sql(table, columns)

            rows = [tuple(row) for row in df.itertuples(index=False)]
            total_rows = len(rows)

            print(f"[LOAD] Inserting {total_rows} rows into `{table}`")

            # Batch inserts
            for i in range(0, total_rows, batch_size):
                batch = rows[i:i + batch_size]
                cursor.executemany(insert_sql, batch)

        # Commit only if everything succeeds
        conn.commit()
        print("[LOAD] All data loaded successfully")

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"[LOAD ERROR] Data load failed: {e}")

    finally:
        cursor.close()
        conn.close()
