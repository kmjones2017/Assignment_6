from prefect import task
import pandas as pd
from tasks.db import get_db_connection


# -------------------------------------------------
# Fact table (Pandas)
# -------------------------------------------------
@task
def build_orders_fact_pandas() -> pd.DataFrame:
    """
    Builds the orders fact table using Pandas.
    Mirrors transformation.py logic exactly.
    """
    conn = get_db_connection()

    customers = pd.read_sql("SELECT * FROM customers", conn)
    orders = pd.read_sql("SELECT * FROM orders", conn)
    order_items = pd.read_sql("SELECT * FROM order_items", conn)
    drug_batches = pd.read_sql("SELECT * FROM drug_batches", conn)
    drugs = pd.read_sql("SELECT * FROM drugs", conn)

    conn.close()

    # ---- Merge lineage (intentional LEFT joins) ----
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

    print(f"[PANDAS FACT] orders_fact rows: {len(final_df)}")
    return final_df


# -------------------------------------------------
# Pandas aggregations
# -------------------------------------------------
@task
def run_pandas_aggregations(orders_fact: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Performs all Pandas-based analytics found in analytics.py.
    Returns a dictionary of DataFrames for downstream validation.
    """
    df = orders_fact.copy()

    # ---- Derived metric ----
    df["revenue"] = df["quantity"] * df["unit_price"]

    # ---- Revenue by product ----
    revenue_by_product = (
        df
        .groupby(["drug_id", "drug_name"], as_index=False)
        .agg(
            quantity_sold=("quantity", "sum"),
            total_revenue=("revenue", "sum")
        )
    )

    # ---- Top products ----
    top_by_quantity = revenue_by_product.sort_values(
        "quantity_sold", ascending=False
    )

    top_by_revenue = revenue_by_product.sort_values(
        "total_revenue", ascending=False
    )

    # ---- Customer segmentation ----
    customer_spend = (
        df
        .groupby(["customer_id", "name"], as_index=False)
        .agg(
            total_spent=("revenue", "sum"),
            order_count=("order_id", "nunique")
        )
        .rename(columns={"name": "customer_name"})
    )

    def segment_customer(spend: float) -> str:
        if spend >= 50000:
            return "High"
        elif spend >= 20000:
            return "Medium"
        else:
            return "Low"

    customer_spend["segment"] = customer_spend["total_spent"].apply(segment_customer)

    print(f"[PANDAS AGG] revenue_by_product rows: {len(revenue_by_product)}")
    print(f"[PANDAS AGG] top_products_by_quantity rows: {len(top_by_quantity)}")
    print(f"[PANDAS AGG] top_products_by_revenue rows: {len(top_by_revenue)}")
    print(f"[PANDAS AGG] customer_segmentation rows: {len(customer_spend)}")


    return {
        "revenue_by_product": revenue_by_product,
        "top_products_by_quantity": top_by_quantity,
        "top_products_by_revenue": top_by_revenue,
        "customer_segmentation": customer_spend,
    }
