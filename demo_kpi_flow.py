"""
flows/demo_kpi_flow.py

DEMO flow:
- Reads general KPIs from pharma_db tables
- Creates MySQL table `kpis` if missing
- Inserts 1 KPI snapshot row per run
- Saves same KPI snapshot to Desktop as CSV
- Logs detailed info about every step
"""

import os
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine, text

# Load .env from current working directory (run from project root!)
load_dotenv()

# ---------- SETTINGS ----------
DB_NAME = "pharma_db"
KPI_TABLE = "kpis"
# -----------------------------


def get_mysql_engine():
    """
    Creates a SQLAlchemy engine using env vars (recommended):
      MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
    """
    host = os.getenv("MYSQL_HOST", "localhost")
    port = os.getenv("MYSQL_PORT", "3306")
    user = os.getenv("MYSQL_USER", "root")
    pwd = os.getenv("MYSQL_PASSWORD", "")
    db = os.getenv("MYSQL_DB", DB_NAME)

    # Log connection info (safe: does NOT print password)
    logger = get_run_logger()
    logger.info(
        f"MySQL connect -> host={host} port={port} user={user} db={db} "
        f"password_set={'YES' if pwd else 'NO'}"
    )

    url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


@task(retries=2, retry_delay_seconds=5, log_prints=True)
def create_kpi_table() -> None:
    logger = get_run_logger()
    engine = get_mysql_engine()

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {KPI_TABLE} (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        run_ts DATETIME NOT NULL,

        customers_count BIGINT,
        orders_count BIGINT,
        order_items_count BIGINT,
        drugs_count BIGINT,
        drug_batches_count BIGINT,
        suppliers_count BIGINT,

        fact_orders_enriched_count BIGINT,
        analytics_sales_by_customer_count BIGINT,
        analytics_sales_by_drug_count BIGINT,

        total_quantity BIGINT,
        total_revenue DECIMAL(18,2),

        first_order_date DATETIME NULL,
        last_order_date DATETIME NULL
    );
    """

    logger.info(f"Creating KPI table if not exists: {KPI_TABLE}")
    logger.debug(f"DDL:\n{ddl}")

    with engine.begin() as conn:
        conn.execute(text(ddl))

    logger.info(f"Table ready: {KPI_TABLE}")


@task(retries=2, retry_delay_seconds=5, log_prints=True)
def compute_kpis() -> dict:
    logger = get_run_logger()
    engine = get_mysql_engine()

    def scalar(sql: str):
        logger.debug(f"SQL -> {sql}")
        with engine.connect() as conn:
            return conn.execute(text(sql)).scalar()

    logger.info("Computing KPIs from tables...")

    # Basic counts
    customers_count = int(scalar("SELECT COUNT(*) FROM customers"))
    orders_count = int(scalar("SELECT COUNT(*) FROM orders"))
    order_items_count = int(scalar("SELECT COUNT(*) FROM order_items"))
    drugs_count = int(scalar("SELECT COUNT(*) FROM drugs"))
    drug_batches_count = int(scalar("SELECT COUNT(*) FROM drug_batches"))
    suppliers_count = int(scalar("SELECT COUNT(*) FROM suppliers"))

    fact_orders_enriched_count = int(scalar("SELECT COUNT(*) FROM fact_orders_enriched"))
    analytics_sales_by_customer_count = int(scalar("SELECT COUNT(*) FROM analytics_sales_by_customer"))
    analytics_sales_by_drug_count = int(scalar("SELECT COUNT(*) FROM analytics_sales_by_drug"))

    # Revenue-style KPIs from order_items (handles NULL safely)
    total_quantity = int(scalar("SELECT COALESCE(SUM(quantity),0) FROM order_items") or 0)
    total_revenue = float(scalar("SELECT COALESCE(SUM(quantity * unit_price),0) FROM order_items") or 0)

    # Try to find a date column automatically in orders
    date_candidates = ["order_date", "created_at", "order_datetime", "order_time", "date"]
    found_date_col = None

    for c in date_candidates:
        try:
            scalar(f"SELECT {c} FROM orders LIMIT 1")
            found_date_col = c
            break
        except Exception:
            continue

    if found_date_col:
        logger.info(f"Using orders date column: {found_date_col}")
        first_order_date = scalar(f"SELECT MIN({found_date_col}) FROM orders")
        last_order_date = scalar(f"SELECT MAX({found_date_col}) FROM orders")
    else:
        logger.warning("No known date column found in orders. Saving NULL dates.")
        first_order_date = None
        last_order_date = None

    logger.info(
        "KPI results:\n"
        f"- customers={customers_count:,}\n"
        f"- orders={orders_count:,}\n"
        f"- order_items={order_items_count:,}\n"
        f"- drugs={drugs_count:,}\n"
        f"- drug_batches={drug_batches_count:,}\n"
        f"- suppliers={suppliers_count:,}\n"
        f"- fact_orders_enriched={fact_orders_enriched_count:,}\n"
        f"- analytics_sales_by_customer={analytics_sales_by_customer_count:,}\n"
        f"- analytics_sales_by_drug={analytics_sales_by_drug_count:,}\n"
        f"- total_quantity={total_quantity:,}\n"
        f"- total_revenue={total_revenue:,.2f}\n"
        f"- first_order_date={first_order_date}\n"
        f"- last_order_date={last_order_date}"
    )

    return {
        "run_ts": datetime.now(),
        "customers_count": customers_count,
        "orders_count": orders_count,
        "order_items_count": order_items_count,
        "drugs_count": drugs_count,
        "drug_batches_count": drug_batches_count,
        "suppliers_count": suppliers_count,
        "fact_orders_enriched_count": fact_orders_enriched_count,
        "analytics_sales_by_customer_count": analytics_sales_by_customer_count,
        "analytics_sales_by_drug_count": analytics_sales_by_drug_count,
        "total_quantity": total_quantity,
        "total_revenue": total_revenue,
        "first_order_date": first_order_date,
        "last_order_date": last_order_date,
    }


@task(retries=2, retry_delay_seconds=5, log_prints=True)
def write_kpis_to_mysql(kpis: dict) -> None:
    logger = get_run_logger()
    engine = get_mysql_engine()

    insert_sql = f"""
    INSERT INTO {KPI_TABLE} (
        run_ts,
        customers_count, orders_count, order_items_count,
        drugs_count, drug_batches_count, suppliers_count,
        fact_orders_enriched_count, analytics_sales_by_customer_count, analytics_sales_by_drug_count,
        total_quantity, total_revenue,
        first_order_date, last_order_date
    ) VALUES (
        :run_ts,
        :customers_count, :orders_count, :order_items_count,
        :drugs_count, :drug_batches_count, :suppliers_count,
        :fact_orders_enriched_count, :analytics_sales_by_customer_count, :analytics_sales_by_drug_count,
        :total_quantity, :total_revenue,
        :first_order_date, :last_order_date
    );
    """

    logger.info(f"Inserting KPI row into MySQL table: {KPI_TABLE}")
    logger.debug(f"INSERT SQL:\n{insert_sql}")

    with engine.begin() as conn:
        conn.execute(text(insert_sql), kpis)

    logger.info("Insert complete (1 row).")


@task(log_prints=True)
def save_kpis_to_desktop_csv(kpis: dict) -> str:
    logger = get_run_logger()

    desktop = os.path.join(os.path.expanduser("~"), "Desktop")
    os.makedirs(desktop, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(desktop, f"kpis_{ts}.csv")

    df = pd.DataFrame([kpis])
    df.to_csv(out_path, index=False)

    logger.info(f"CSV saved to Desktop: {out_path}")
    logger.info(f"CSV columns: {list(df.columns)}")
    logger.info(f"CSV preview:\n{df.head(1).to_string(index=False)}")

    return out_path


@flow(name="DEMO - General KPIs (MySQL + Desktop CSV)", log_prints=True)
def demo_general_kpis_flow():
    logger = get_run_logger()
    logger.info("Starting DEMO KPI flow...")

    create_kpi_table()
    kpis = compute_kpis()
    write_kpis_to_mysql(kpis)
    csv_path = save_kpis_to_desktop_csv(kpis)

    logger.info(f"DEMO KPI flow finished successfully. CSV: {csv_path}")
    return csv_path


if __name__ == "__main__":
    demo_general_kpis_flow()
