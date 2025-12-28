# analytics.py

import os
import pandas as pd
from transformation_v2 import build_orders_fact

# -------------------------
# Load transformed data
# -------------------------
df = build_orders_fact()

# -------------------------
# Add derived metrics
# -------------------------
df["revenue"] = df["quantity"] * df["unit_price"]

# -------------------------
# 1. Total revenue by product
# -------------------------
revenue_by_product = (
    df
    .groupby(["drug_id", "drug_name"], as_index=False)
    .agg(
        quantity_sold=("quantity", "sum"),
        total_revenue=("revenue", "sum")
    )
)

# -------------------------
# 2. Top-selling products
# -------------------------
product_sales = (
    df
    .groupby(["drug_id", "drug_name"], as_index=False)
    .agg(
        quantity_sold=("quantity", "sum"),
        total_revenue=("revenue", "sum")
    )
)

top_products_by_quantity = product_sales.sort_values(
    by="quantity_sold",
    ascending=False
)

top_products_by_revenue = product_sales.sort_values(
    by="total_revenue",
    ascending=False
)

# -------------------------
# 3. Customer segmentation
# -------------------------
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

# -------------------------
# Validation
# -------------------------

# Basic Data Integrity Checks
assert not df["quantity"].isnull().any(), "Null quantities found"
assert not df["unit_price"].isnull().any(), "Null prices found"
assert (df["quantity"] >= 0).all(), "Negative quantities found"
assert (df["unit_price"] >= 0).all(), "Negative prices found"

# Revenue Consistency Check
assert (df["revenue"] == df["quantity"] * df["unit_price"]).all(), \
    "Revenue calculation mismatch"

# Aggregation Reconciliation Check
total_revenue_raw = df["revenue"].sum()
total_revenue_agg = revenue_by_product["total_revenue"].sum()

assert round(total_revenue_raw, 2) == round(total_revenue_agg, 2), \
    "Revenue mismatch between raw and aggregated data"

# Customer Segmentation Coverage Check
assert customer_spend["segment"].isin(["High", "Medium", "Low"]).all(), \
    "Unexpected customer segment found"

# -------------------------
# Export analytics outputs
# -------------------------
OUTPUT_DIR = "analytics_outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

revenue_by_product.to_csv(
    f"{OUTPUT_DIR}/revenue_by_product.csv",
    index=False
)

top_products_by_quantity.to_csv(
    f"{OUTPUT_DIR}/top_products_by_quantity.csv",
    index=False
)

top_products_by_revenue.to_csv(
    f"{OUTPUT_DIR}/top_products_by_revenue.csv",
    index=False
)

customer_spend.to_csv(
    f"{OUTPUT_DIR}/customer_segmentation.csv",
    index=False
)

# -------------------------
# Optional: display outputs
# -------------------------
if __name__ == "__main__":
    print(customer_spend["segment"].value_counts())

    print("\n--- Revenue by Product ---")
    print(revenue_by_product.head())

    print("\n--- Top Products by Quantity ---")
    print(top_products_by_quantity.head())

    print("\n--- Top Products by Revenue ---")
    print(top_products_by_revenue.head())

    print("\n--- Customer Segmentation ---")
    print(customer_spend.head())

    print("Analytics outputs successfully written to CSV.")
