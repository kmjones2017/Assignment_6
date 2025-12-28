# analytics.py

import pandas as pd
from transformation import build_orders_fact

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
    .agg(total_revenue=("revenue", "sum"))
)

# -------------------------
# 2. Top-selling products
# -------------------------
product_sales = (
    df
    .groupby(["drug_id", "drug_name"], as_index=False)
    .agg(
        total_quantity=("quantity", "sum"),
        total_revenue=("revenue", "sum")
    )
)

top_products_by_quantity = product_sales.sort_values(
    by="total_quantity",
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
    .groupby("customer_id", as_index=False)
    .agg(total_spend=("revenue", "sum"))
)

def segment_customer(spend: float) -> str:
    if spend >= 50000:
        return "High"
    elif spend >= 20000:
        return "Medium"
    else:
        return "Low"

customer_spend["segment"] = customer_spend["total_spend"].apply(segment_customer)

# -------------------------
# Optional: display outputs
# -------------------------
if __name__ == "__main__":
    print("\n--- Revenue by Product ---")
    print(revenue_by_product.head())

    print("\n--- Top Products by Quantity ---")
    print(top_products_by_quantity.head())

    print("\n--- Top Products by Revenue ---")
    print(top_products_by_revenue.head())

    print("\n--- Customer Segmentation ---")
    print(customer_spend.head())
