from prefect import task
import pandas as pd
from sqlalchemy import create_engine, text
from config import db_config

import os

FAIL_ONCE_FLAG = r"C:\p5\prefect_etl\fail_once.flag"


@task(name="Write DataFrame To MySQL", retries=3, retry_delay_seconds=5)
def write_dataframe(df: pd.DataFrame, table_name: str) -> None:
    """
    Writes a pandas DataFrame into MySQL.
    - Uses a transaction
    - Truncates the target table first (so each run is clean)
    - Retries on failure (Prefect feature)
    """

    # --- Failure simulation: fail once, then succeed on retry ---
   # --- if os.path.exists(FAIL_ONCE_FLAG):---
   # ---    os.remove(FAIL_ONCE_FLAG)---
     # ---  raise RuntimeError("Simulated failure (fail once) - Prefect should retry and succeed.")---

    user = db_config["user"]
    password = db_config["password"]
    host = db_config["host"]
    database = db_config["database"]

    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:3306/{database}")

    # Rename columns to match our table schema
    df_to_write = df.rename(columns={
        "name": "customer_name"
    })

    # Keep only columns that exist in the target table, in correct order
    ordered_cols = [
        "order_id", "customer_id", "order_date",
        "customer_name", "customer_type", "email", "phone", "address",
        "order_item_id", "batch_id", "quantity", "unit_price",
        "drug_id", "batch_number", "manufacture_date", "expiration_date", "quantity_produced",
        "drug_name", "dosage_form", "strength_mg", "price"
    ]
    df_to_write = df_to_write[ordered_cols]

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name};"))
        df_to_write.to_sql(table_name, con=engine, if_exists="append", index=False)

    print(f"✅ Wrote {len(df_to_write)} rows into {table_name}")
