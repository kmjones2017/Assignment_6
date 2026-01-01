"""
flows/pipeline_flow.py

PIPELINE flow (matches your diagram):

Raw CSV Files -> Load to MySQL -> DATABASE (MySQL)
    -> SQL Fact/Analytics (JOINs)     -> SQL Aggregations (GROUP BY, SUM, etc.)
    -> Pandas Fact/Analytics (JOINs)  -> Pandas Aggregations (groupby, agg)
    -> Validation Task (Compare SQL vs Pandas aggregates)
    -> Persist SQL Results (CSV / artifact)
    -> Final Output / Report (trusted metrics)

Run:
  python -m flows.pipeline_flow

If you want this flow to also load CSVs into MySQL:
  pipeline(load_csv=True, csv_dir=r"C:\path\to\raw_csvs")
"""

import os
from datetime import datetime
from math import isclose
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine, inspect, text

# Load .env from project root (run from project root!)
load_dotenv()

# ---------- SETTINGS ----------
DB_NAME = os.getenv("MYSQL_DB", "pharma_db")

# If you already loaded CSVs into MySQL, keep this False.
# If you want this flow to load CSVs -> MySQL, set True and provide csv_dir.
LOAD_CSV_TO_MYSQL_DEFAULT = False

# Your project table names
T_CUSTOMERS = "customers"
T_ORDERS = "orders"
T_ORDER_ITEMS = "order_items"
T_DRUGS = "drugs"
T_DRUG_BATCHES = "drug_batches"
# -----------------------------


# ---------- MySQL engine ----------
def get_mysql_engine():
    host = os.getenv("MYSQL_HOST", "localhost")
    port = os.getenv("MYSQL_PORT", "3306")
    user = os.getenv("MYSQL_USER", "root")
    pwd = os.getenv("MYSQL_PASSWORD", "")
    db = os.getenv("MYSQL_DB", DB_NAME)

    logger = get_run_logger()
    logger.info(
        f"MySQL connect -> host={host} port={port} user={user} db={db} "
        f"password_set={'YES' if pwd else 'NO'}"
    )

    url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


# ---------- helpers (schema detection) ----------
def _cols(engine, table: str) -> set[str]:
    insp = inspect(engine)
    return {c["name"] for c in insp.get_columns(table)}


def _pick_col(engine, table: str, candidates: list[str], required: bool = True) -> str | None:
    existing = _cols(engine, table)
    for c in candidates:
        if c in existing:
            return c
    if required:
        raise ValueError(
            f"Missing expected columns in `{table}`. Tried: {candidates}. Found: {sorted(existing)}"
        )
    return None


def _table_exists(engine, table: str) -> bool:
    return inspect(engine).has_table(table)


# ---------- 1) Raw CSV -> Load to MySQL (schema + data) ----------
@task(retries=2, retry_delay_seconds=5, log_prints=True)
def load_csv_folder_to_mysql(csv_dir: str) -> None:
    """
    Demo CSV loader:
    - Reads every *.csv in csv_dir
    - Loads into MySQL table with same name as file (without .csv)
    - Replaces table each run (good for demo)
    """
    logger = get_run_logger()
    engine = get_mysql_engine()

    p = Path(csv_dir)
    if not p.exists():
        raise FileNotFoundError(f"CSV folder not found: {csv_dir}")

    csv_files = sorted(p.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in: {csv_dir}")

    logger.info(f"Loading {len(csv_files)} CSV files into MySQL (replace tables each run)...")

    for f in csv_files:
        table = f.stem.lower()
        df = pd.read_csv(f)

        logger.info(f"-> {f.name}  rows={len(df):,}  cols={len(df.columns)}  table={table}")
        df.to_sql(table, engine, if_exists="replace", index=False)

    logger.info("CSV -> MySQL load completed.")


@task(log_prints=True)
def ensure_mysql_ready(load_csv: bool, csv_dir: str | None) -> None:
    """
    Ensures the DB has required tables.
    If load_csv=True, loads CSVs first.
    """
    logger = get_run_logger()
    engine = get_mysql_engine()

    if load_csv:
        if not csv_dir:
            raise ValueError("load_csv=True but csv_dir is empty. Provide csv_dir to the flow.")
        load_csv_folder_to_mysql(csv_dir)

    # Required tables for this pipeline (because we join via drug_batches)
    required = [T_ORDERS, T_ORDER_ITEMS, T_DRUG_BATCHES, T_DRUGS]
    missing = [t for t in required if not _table_exists(engine, t)]

    if missing:
        raise ValueError(
            f"Missing required MySQL tables: {missing}. "
            f"Load them first (your earlier day flows) OR run pipeline(load_csv=True, csv_dir=...)."
        )

    logger.info("MySQL database is ready (required tables exist).")


# ---------- 2) SQL Fact / Analytics Dataset (JOINs) ----------
@task(retries=2, retry_delay_seconds=5, log_prints=True)
def sql_fact_orders() -> pd.DataFrame:
    """
    Builds FACT dataset in SQL using JOINs via drug_batches:
      order_items.batch_id -> drug_batches.batch_id -> drug_batches.drug_id -> drugs.drug_id
    Returns DataFrame with: order_key, drug_name, quantity, unit_price, revenue
    """
    logger = get_run_logger()
    engine = get_mysql_engine()

    # Orders keys
    orders_pk = _pick_col(engine, T_ORDERS, ["order_id", "id"])
    oi_order_fk = _pick_col(engine, T_ORDER_ITEMS, ["order_id", "orders_id", "order_fk", orders_pk])

    # Order items -> batches
    batch_pk = _pick_col(engine, T_DRUG_BATCHES, ["batch_id", "id"])
    oi_batch_fk = _pick_col(engine, T_ORDER_ITEMS, ["batch_id", "drug_batch_id", "batch_fk", batch_pk])

    # Batches -> drugs
    drug_pk = _pick_col(engine, T_DRUGS, ["drug_id", "id"])
    batch_drug_fk = _pick_col(engine, T_DRUG_BATCHES, ["drug_id", "drugs_id", "drug_fk", drug_pk])

    # Measures + name
    qty_col = _pick_col(engine, T_ORDER_ITEMS, ["quantity", "qty"])
    price_col = _pick_col(engine, T_ORDER_ITEMS, ["unit_price", "price", "unitprice"])
    drug_name_col = _pick_col(engine, T_DRUGS, ["drug_name", "name", "drug"])

    sql = f"""
    SELECT
        oi.{oi_order_fk} AS order_key,
        d.{drug_name_col} AS drug_name,
        oi.{qty_col} AS quantity,
        oi.{price_col} AS unit_price,
        (COALESCE(oi.{qty_col},0) * COALESCE(oi.{price_col},0)) AS revenue
    FROM {T_ORDER_ITEMS} oi
    JOIN {T_ORDERS} o
        ON o.{orders_pk} = oi.{oi_order_fk}
    JOIN {T_DRUG_BATCHES} b
        ON b.{batch_pk} = oi.{oi_batch_fk}
    JOIN {T_DRUGS} d
        ON d.{drug_pk} = b.{batch_drug_fk}
    """

    logger.info("Building SQL fact dataset (JOINs via drug_batches)...")
    df = pd.read_sql_query(sql, engine)

    logger.info(f"SQL fact rows: {len(df):,}")
    logger.info(f"SQL fact preview:\n{df.head(5).to_string(index=False)}")
    return df


# ---------- 3) Pandas Fact / Analytics Dataset (JOINs) ----------
@task(retries=2, retry_delay_seconds=5, log_prints=True)
def pandas_fact_orders() -> pd.DataFrame:
    """
    Builds the same FACT dataset in Pandas using merges via drug_batches.
    Returns DataFrame with: order_key, drug_name, quantity, unit_price, revenue
    """
    logger = get_run_logger()
    engine = get_mysql_engine()

    orders_pk = _pick_col(engine, T_ORDERS, ["order_id", "id"])
    oi_order_fk = _pick_col(engine, T_ORDER_ITEMS, ["order_id", "orders_id", "order_fk", orders_pk])

    batch_pk = _pick_col(engine, T_DRUG_BATCHES, ["batch_id", "id"])
    oi_batch_fk = _pick_col(engine, T_ORDER_ITEMS, ["batch_id", "drug_batch_id", "batch_fk", batch_pk])

    drug_pk = _pick_col(engine, T_DRUGS, ["drug_id", "id"])
    batch_drug_fk = _pick_col(engine, T_DRUG_BATCHES, ["drug_id", "drugs_id", "drug_fk", drug_pk])

    qty_col = _pick_col(engine, T_ORDER_ITEMS, ["quantity", "qty"])
    price_col = _pick_col(engine, T_ORDER_ITEMS, ["unit_price", "price", "unitprice"])
    drug_name_col = _pick_col(engine, T_DRUGS, ["drug_name", "name", "drug"])

    logger.info("Loading base tables into Pandas (JOINs via drug_batches)...")

    oi = pd.read_sql_query(
        f"""
        SELECT
          {oi_order_fk} AS order_key,
          {oi_batch_fk} AS batch_key,
          {qty_col} AS quantity,
          {price_col} AS unit_price
        FROM {T_ORDER_ITEMS}
        """,
        engine,
    )

    o = pd.read_sql_query(
        f"SELECT {orders_pk} AS order_key FROM {T_ORDERS}",
        engine,
    )

    b = pd.read_sql_query(
        f"SELECT {batch_pk} AS batch_key, {batch_drug_fk} AS drug_key FROM {T_DRUG_BATCHES}",
        engine,
    )

    d = pd.read_sql_query(
        f"SELECT {drug_pk} AS drug_key, {drug_name_col} AS drug_name FROM {T_DRUGS}",
        engine,
    )

    fact = (
        oi.merge(o, on="order_key", how="inner")
          .merge(b, on="batch_key", how="inner")
          .merge(d, on="drug_key", how="inner")
    )

    fact["quantity"] = fact["quantity"].fillna(0)
    fact["unit_price"] = fact["unit_price"].fillna(0)
    fact["revenue"] = fact["quantity"] * fact["unit_price"]

    logger.info(f"Pandas fact rows: {len(fact):,}")
    logger.info(f"Pandas fact preview:\n{fact.head(5).to_string(index=False)}")

    return fact[["order_key", "drug_name", "quantity", "unit_price", "revenue"]]


# ---------- 4) SQL Aggregations (GROUP BY, SUM, etc.) ----------
@task(retries=2, retry_delay_seconds=5, log_prints=True)
def sql_aggregations_by_drug() -> pd.DataFrame:
    """
    Aggregates in SQL (GROUP BY drug_name) via drug_batches join path.
    Output columns:
      drug_name, line_count, total_quantity, total_revenue
    """
    logger = get_run_logger()
    engine = get_mysql_engine()

    orders_pk = _pick_col(engine, T_ORDERS, ["order_id", "id"])
    oi_order_fk = _pick_col(engine, T_ORDER_ITEMS, ["order_id", "orders_id", "order_fk", orders_pk])

    batch_pk = _pick_col(engine, T_DRUG_BATCHES, ["batch_id", "id"])
    oi_batch_fk = _pick_col(engine, T_ORDER_ITEMS, ["batch_id", "drug_batch_id", "batch_fk", batch_pk])

    drug_pk = _pick_col(engine, T_DRUGS, ["drug_id", "id"])
    batch_drug_fk = _pick_col(engine, T_DRUG_BATCHES, ["drug_id", "drugs_id", "drug_fk", drug_pk])

    qty_col = _pick_col(engine, T_ORDER_ITEMS, ["quantity", "qty"])
    price_col = _pick_col(engine, T_ORDER_ITEMS, ["unit_price", "price", "unitprice"])
    drug_name_col = _pick_col(engine, T_DRUGS, ["drug_name", "name", "drug"])

    sql = f"""
    SELECT
        d.{drug_name_col} AS drug_name,
        COUNT(*) AS line_count,
        COALESCE(SUM(oi.{qty_col}),0) AS total_quantity,
        COALESCE(SUM(oi.{qty_col} * oi.{price_col}),0) AS total_revenue
    FROM {T_ORDER_ITEMS} oi
    JOIN {T_ORDERS} o
        ON o.{orders_pk} = oi.{oi_order_fk}
    JOIN {T_DRUG_BATCHES} b
        ON b.{batch_pk} = oi.{oi_batch_fk}
    JOIN {T_DRUGS} d
        ON d.{drug_pk} = b.{batch_drug_fk}
    GROUP BY d.{drug_name_col}
    ORDER BY total_revenue DESC
    """

    logger.info("Running SQL aggregations (GROUP BY drug) via drug_batches...")
    df = pd.read_sql_query(sql, engine)

    logger.info(f"SQL agg rows: {len(df):,}")
    logger.info(f"SQL agg preview:\n{df.head(10).to_string(index=False)}")
    return df


# ---------- 5) Pandas Aggregations (groupby, agg) ----------
@task(retries=2, retry_delay_seconds=5, log_prints=True)
def pandas_aggregations_by_drug(fact_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pandas aggregation from Pandas fact dataset.
    Output columns:
      drug_name, line_count, total_quantity, total_revenue
    """
    logger = get_run_logger()

    logger.info("Running Pandas aggregations (groupby drug)...")

    df = (
        fact_df.groupby("drug_name", as_index=False)
        .agg(
            line_count=("drug_name", "size"),
            total_quantity=("quantity", "sum"),
            total_revenue=("revenue", "sum"),
        )
        .sort_values("total_revenue", ascending=False)
    )

    df["total_quantity"] = df["total_quantity"].fillna(0).astype(int)
    df["total_revenue"] = df["total_revenue"].fillna(0).astype(float)

    logger.info(f"Pandas agg rows: {len(df):,}")
    logger.info(f"Pandas agg preview:\n{df.head(10).to_string(index=False)}")
    return df


# ---------- 6) Validation Task (Compare SQL vs Pandas Aggregates) ----------
@task(retries=1, retry_delay_seconds=2, log_prints=True)
def validate_sql_vs_pandas_aggregates(sql_agg: pd.DataFrame, pd_agg: pd.DataFrame, abs_tol: float = 0.01) -> None:
    """
    Validates same aggregates by drug_name.
    - If mismatch -> raise error (Prefect run fails)
    """
    logger = get_run_logger()

    s = sql_agg.copy()
    p = pd_agg.copy()

    s["drug_name"] = s["drug_name"].astype(str).str.strip()
    p["drug_name"] = p["drug_name"].astype(str).str.strip()

    merged = s.merge(p, on="drug_name", how="outer", suffixes=("_sql", "_pd"))

    for col in ["line_count", "total_quantity", "total_revenue"]:
        merged[f"{col}_sql"] = merged[f"{col}_sql"].fillna(0)
        merged[f"{col}_pd"] = merged[f"{col}_pd"].fillna(0)

    mismatches = []

    bad_count = merged[merged["line_count_sql"].astype(int) != merged["line_count_pd"].astype(int)]
    if len(bad_count) > 0:
        mismatches.append(("line_count", bad_count[["drug_name", "line_count_sql", "line_count_pd"]].head(10)))

    bad_qty = merged[merged["total_quantity_sql"].astype(int) != merged["total_quantity_pd"].astype(int)]
    if len(bad_qty) > 0:
        mismatches.append(("total_quantity", bad_qty[["drug_name", "total_quantity_sql", "total_quantity_pd"]].head(10)))

    rev_bad_rows = []
    for _, r in merged.iterrows():
        a = float(r["total_revenue_sql"])
        b = float(r["total_revenue_pd"])
        if not isclose(a, b, abs_tol=abs_tol):
            rev_bad_rows.append((r["drug_name"], a, b, abs(a - b)))

    if rev_bad_rows:
        mismatches.append(
            ("total_revenue", pd.DataFrame(rev_bad_rows, columns=["drug_name", "sql", "pandas", "abs_diff"]).head(10))
        )

    if mismatches:
        logger.error("VALIDATION FAILED: SQL vs Pandas aggregates mismatch ❌")
        for name, sample in mismatches:
            logger.error(f"Mismatch in {name}. Sample:\n{sample.to_string(index=False)}")
        raise ValueError("Validation failed: SQL and Pandas aggregates do not match.")
    else:
        logger.info("VALIDATION PASSED: SQL vs Pandas aggregates match ✅")


# ---------- 7) Persist SQL Results (CSV / artifact) ----------
@task(log_prints=True)
def persist_sql_results_to_csv(sql_agg: pd.DataFrame) -> str:
    """
    Saves the SQL aggregate results to Desktop as CSV.
    """
    logger = get_run_logger()

    desktop = os.path.join(os.path.expanduser("~"), "Desktop")
    os.makedirs(desktop, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(desktop, f"pipeline_sql_agg_by_drug_{ts}.csv")

    sql_agg.to_csv(out_path, index=False)

    logger.info(f"Persisted SQL results CSV -> {out_path}")
    return out_path


# ---------- 8) Final Output / Report ----------
@task(log_prints=True)
def final_report(sql_csv_path: str, sql_agg: pd.DataFrame) -> dict:
    logger = get_run_logger()

    total_rev = float(sql_agg["total_revenue"].fillna(0).sum()) if len(sql_agg) else 0.0
    total_qty = int(sql_agg["total_quantity"].fillna(0).sum()) if len(sql_agg) else 0

    logger.info("FINAL REPORT (trusted metrics)")
    logger.info(f"- SQL results CSV: {sql_csv_path}")
    logger.info(f"- Total revenue (sum of groups): {total_rev:,.2f}")
    logger.info(f"- Total quantity (sum of groups): {total_qty:,}")
    logger.info(f"- Unique drugs (groups): {len(sql_agg):,}")

    return {
        "sql_results_csv": sql_csv_path,
        "trusted_total_revenue": total_rev,
        "trusted_total_quantity": total_qty,
        "groups": int(len(sql_agg)),
    }


# =========================
# FLOW: pipeline
# =========================
@flow(name="PIPELINE - CSV → MySQL → Fact → Agg → Validate → Report", log_prints=True)
def pipeline(load_csv: bool = LOAD_CSV_TO_MYSQL_DEFAULT, csv_dir: str | None = None) -> dict:
    logger = get_run_logger()
    logger.info("Starting PIPELINE flow (matches diagram)...")

    # 1) Raw CSV -> Load to MySQL (optional)
    ensure_mysql_ready(load_csv=load_csv, csv_dir=csv_dir)

    # 2) SQL Fact / Analytics dataset (JOINs)
    _sql_fact = sql_fact_orders()

    # 3) Pandas Fact / Analytics dataset (JOINs)
    pd_fact = pandas_fact_orders()

    # 4) SQL Aggregations
    sql_agg = sql_aggregations_by_drug()

    # 5) Pandas Aggregations
    pd_agg = pandas_aggregations_by_drug(pd_fact)

    # 6) Validation
    validate_sql_vs_pandas_aggregates(sql_agg, pd_agg)

    # 7) Persist SQL results (CSV / artifact)
    sql_csv_path = persist_sql_results_to_csv(sql_agg)

    # 8) Final output / report
    report = final_report(sql_csv_path, sql_agg)

    logger.info("PIPELINE flow finished successfully ✅")
    return report


if __name__ == "__main__":
    # If you want CSV -> MySQL inside this flow:
    # pipeline(load_csv=True, csv_dir=r"C:\path\to\raw_csvs")
    pipeline()
