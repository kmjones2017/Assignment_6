# flows/day8_analytics_flow.py

from prefect import flow, get_run_logger
from check_mysql import get_mysql_engine
from tasks.analytics.sales_by_drug import sales_by_drug
from tasks.analytics.sales_by_customer import sales_by_customer


@flow(name="Day 8 - Analytics and Reporting", log_prints=True)
def day8_analytics_flow():
    logger = get_run_logger()
    logger.info("Day 8 flow started: building MySQL engine...")

    engine = get_mysql_engine()

    # quick connection check
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
        logger.info("MySQL connection OK (SELECT 1 succeeded).")
    except Exception as e:
        logger.exception(f"MySQL connection FAILED: {e}")
        raise

    logger.info("Running analytics task: sales_by_drug")
    n1 = sales_by_drug(engine)
    logger.info(f"✅ analytics_sales_by_drug rows written: {n1}")

    logger.info("Running analytics task: sales_by_customer")
    n2 = sales_by_customer(engine)
    logger.info(f"✅ analytics_sales_by_customer rows written: {n2}")

    logger.info("Day 8 flow completed successfully.")
    return {"sales_by_drug_rows": n1, "sales_by_customer_rows": n2}


if __name__ == "__main__":
    day8_analytics_flow()
