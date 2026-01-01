from prefect import flow

from tasks.pandas_analytics import (
    build_orders_fact_pandas,
    run_pandas_aggregations
)


@flow(name="Pandas Analytics Subflow")
def pandas_analytics_subflow() -> dict:
    """
    Executes Pandas-based analytics path.
    Returns aggregation results for validation.
    """

    # Build Pandas fact table
    orders_fact_df = build_orders_fact_pandas()

    # Run aggregations using the fact table
    pandas_results = run_pandas_aggregations(orders_fact_df)

    return pandas_results
