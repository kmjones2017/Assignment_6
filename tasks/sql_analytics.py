from prefect import task
import pandas as pd
from tasks.sql_executor import execute_sql_file

@task
def build_orders_fact_sql():
    """
    Executes SQL script to generate orders fact dataset.
    """
    execute_sql_file.fn("db/orders_fact.sql")
    print("[SQL ANALYTICS] orders_fact dataset created")

@task
def run_sql_aggregations() -> dict[str, pd.DataFrame]:
    """
    Executes SQL aggregation script and returns named result sets
    as pandas DataFrames. Assumes stable SELECT order in orders_aggregates.sql.
    """

    results = execute_sql_file.fn(
        "db/orders_aggregates.sql",
        return_results=True
    )

    # Map outputs according to their documented order in orders_aggregates.sql
    # Indexing order matches the order of SELECTs in the SQL file
    revenue_by_product_df = results[0]
    top_products_by_quantity_df = results[1]
    top_products_by_revenue_df = results[2]
    customer_segmentation_df = results[3]

    print(
        "[SQL AGGREGATIONS] Returned "
        f"{len(revenue_by_product_df)} revenue rows, "
        f"{len(top_products_by_quantity_df)} top quantity rows, "
        f"{len(top_products_by_revenue_df)} top revenue rows, "
        f"{len(customer_segmentation_df)} customer segmentation rows"
    )

    return {
        "revenue_by_product": revenue_by_product_df,
        "top_products_by_quantity": top_products_by_quantity_df,
        "top_products_by_revenue": top_products_by_revenue_df,
        "customer_segmentation": customer_segmentation_df
    }

