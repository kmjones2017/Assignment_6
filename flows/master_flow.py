from prefect import flow

from tasks.sql_executor import execute_sql_file
from tasks.load_data import load_all_csv_data
from tasks.sanity_check_ingestion import (
    sanity_check_ingestion,
    sanity_check_business_rules
)
from tasks.validation import validate_sql_vs_pandas
from tasks.persist_sql_outputs import persist_sql_outputs

from flows.sql_analytics_subflow import sql_analytics_subflow
from flows.pandas_analytics_subflow import pandas_analytics_subflow


@flow(name="Pharma ETL Master Flow")
def pharma_etl_master_flow():
    """
    End-to-end ETL pipeline for the pharmaceutical manufacturing dataset.
    """

    # =========================
    # 1. Create schema
    # =========================
    execute_sql_file("db/pharma_schema.sql")
    print("[MASTER] Schema created")

    # =========================
    # 2. Load data (with retries)
    # =========================
    load_all_csv_data()
    print("[MASTER] Data loaded")

    # =========================
    # 3. Ingestion sanity checks
    # =========================
    sanity_check_ingestion()
    print("[MASTER] Ingestion integrity validated")

    # =========================
    # 4. Business rule checks
    # =========================
    sanity_check_business_rules()
    print("[MASTER] Business rules validated")

    # =========================
    # 5. Analytics subflows
    # =========================
    sql_results = sql_analytics_subflow()
    pandas_results = pandas_analytics_subflow()

    # =========================
    # 6. Cross-engine validation
    # =========================
    validate_sql_vs_pandas(
        sql_results=sql_results,
        pandas_results=pandas_results
    )
    print("[MASTER] SQL ↔ Pandas validation passed")

    # =========================
    # 7. Persist SQL outputs
    # =========================
    persist_sql_outputs(
        sql_file_path="db/orders_aggregates.sql",
        output_dir="data/reports"
    )
    print("[MASTER] SQL analytics persisted to CSV")
    print("[MASTER] ETL pipeline completed successfully")


if __name__ == "__main__":
    pharma_etl_master_flow()
