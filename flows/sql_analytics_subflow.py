from prefect import flow

from tasks.sql_analytics import (
    build_orders_fact_sql,
    run_sql_aggregations
)


@flow(name="SQL Analytics Subflow")
def sql_analytics_subflow() -> dict:
    """
    Executes SQL-based analytics path.
    Returns aggregation results for validation.
    """

    # Build fact table (orders_fact)
    build_orders_fact_sql()

    # Run aggregations and collect results
    sql_results = run_sql_aggregations()

    return sql_results
