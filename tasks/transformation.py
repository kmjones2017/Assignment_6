# transformation.py

import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# -------------------------
# Database connection
# -------------------------
def get_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
    )

# -------------------------
# Table loader
# -------------------------
def load_table(table_name: str) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df

# -------------------------
# Minimal schema validation
# -------------------------
def validate_columns(df: pd.DataFrame, required_cols: list[str], table_name: str):
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(f"{table_name} missing required columns: {missing}")

# -------------------------
# Core transformation
# -------------------------
def build_orders_fact() -> pd.DataFrame:
    """
    Builds a business-ready fact table by joining customers, orders,
    order_items, drug_batches, and drugs.
    """

    # ---- Load tables ----
    customers = load_table("customers")
    orders = load_table("orders")
    order_items = load_table("order_items")
    drug_batches = load_table("drug_batches")
    drugs = load_table("drugs")

    # ---- Validate minimal schemas ----
    validate_columns(customers, ["customer_id", "name"], "customers")
    validate_columns(orders, ["order_id", "customer_id"], "orders")
    validate_columns(
        order_items,
        ["order_id", "batch_id", "quantity", "unit_price"],
        "order_items"
    )
    validate_columns(drug_batches, ["batch_id", "drug_id"], "drug_batches")
    validate_columns(drugs, ["drug_id", "drug_name"], "drugs")

    # ---- Merge lineage ----
    orders_enriched = orders.merge(
        customers,
        on="customer_id",
        how="left"
    )

    orders_items = orders_enriched.merge(
        order_items,
        on="order_id",
        how="left"
    )

    orders_batches = orders_items.merge(
        drug_batches,
        on="batch_id",
        how="left"
    )

    final_df = orders_batches.merge(
        drugs,
        on="drug_id",
        how="left"
    )

    return final_df


# -------------------------
# Optional: local debug run
# -------------------------
if __name__ == "__main__":
    df = build_orders_fact()
    print(f"Final row count: {len(df)}")
    print(df.head())
