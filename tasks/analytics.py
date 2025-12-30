# analytics.py

import os
import pandas as pd
from prefect import task, flow
from transformation import build_orders_fact

# -------------------------
# Derived metrics
# -------------------------
@task
def add_revenue(df: pd.DataFrame) -> pd.DataFrame:
    df["revenue"] = df["quantity"] * df["unit_price"]
    return df

# -------------------------
# Aggregations
# -------------------------
@task
def revenue_by_product(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df
        .groupby(["drug_id", "drug_name"], as_index=False)
        .agg(
            quantity_sold=("quantity", "sum"),
            total_revenue=("revenue", "sum")
        )
    )

@task
def top_products(df: pd.DataFrame):
    product_sales = (
        df
        .groupby(["drug_id", "drug_name"], as_index=False)
        .agg(
            quantity_sold=("quantity", "sum"),
            total_revenue=("revenue", "sum")
        )
    )

    return (
        product_sales.sort_values("quantity_sold", ascending=False),
        product_sales.sort_values("total_revenue", ascending=False),
    )

@task
def customer_segmentation(df: pd.DataFrame) -> pd.DataFrame:
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
    return customer_spend

# -------------------------
# Validation
# -------------------------
@task
def validate_analytics(df, revenue_by_product_df, customer_spend_df):
    assert not df["quantity"].isnull().any(), "Null quantities found"
    assert not df["unit_price"].isnull().any(), "Null prices found"
    assert (df["quantity"] >= 0).all(), "Negative quantities found"
    assert (df["unit_price"] >= 0).all(), "Negative prices found"

    assert (df["revenue"] == df["quantity"] * df["unit_price"]).all(), \
        "Revenue calculation mismatch"

    total_revenue_raw = df["revenue"].sum()
    total_revenue_agg = revenue_by_product_df["total_revenue"].sum()

    assert round(total_revenue_raw, 2) == round(total_revenue_agg, 2), \
        "Revenue mismatch between raw and aggregated data"

    assert customer_spend_df["segment"].isin(["High", "Medium", "Low"]).all(), \
        "Unexpected customer segment found"

# -------------------------
# Export
# -------------------------
@task
def export_outputs(
    revenue_by_product_df,
    top_qty_df,
    top_rev_df,
    customer_spend_df
):
    output_dir = "analytics_outputs"
    os.makedirs(output_dir, exist_ok=True)

    revenue_by_product_df.to_csv(
        f"{output_dir}/revenue_by_product.csv",
        index=False
    )

    top_qty_df.to_csv(
        f"{output_dir}/top_products_by_quantity.csv",
        index=False
    )

    top_rev_df.to_csv(
        f"{output_dir}/top_products_by_revenue.csv",
        index=False
    )

    customer_spend_df.to_csv(
        f"{output_dir}/customer_segmentation.csv",
        index=False
    )

# -------------------------
# Analytics Flow
# -------------------------
@flow
def analytics_flow():
    df = build_orders_fact()
    df = add_revenue(df)

    revenue_df = revenue_by_product(df)
    top_qty_df, top_rev_df = top_products(df)
    customer_df = customer_segmentation(df)

    validate_analytics(df, revenue_df, customer_df)
    export_outputs(revenue_df, top_qty_df, top_rev_df, customer_df)


if __name__ == "__main__":
    analytics_flow()
