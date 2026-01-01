from prefect import task
from tasks.sql_executor import execute_sql_file

@task(name="Sanity Check Ingestion")
def sanity_check_ingestion():
    """
    Executes ingestion validation SQL.
    Raises ValueError if any referential integrity violations are found.
    """

    results = execute_sql_file.fn(
        "db/pharma_ingestion_validation.sql",
        return_results=True
    )

    # Skip schema/metadata queries; only enforce violation checks
    violation_sets = results[-7:]  # one per FK validation query

    for idx, result_set in enumerate(violation_sets, start=1):
        if not result_set.empty:
            raise ValueError(
                f"Ingestion sanity check failed: "
                f"referential integrity violation detected "
                f"in check #{idx}. Rows found: {len(result_set)}"
            )

    print("[INGESTION SANITY] All ingestion integrity checks passed.")

@task(name="Sanity Check Business Rules")
def sanity_check_business_rules():
    """
    Executes business-rule sanity checks.
    Any returned rows indicate invalid or suspicious business data.
    """

    results = execute_sql_file.fn(
        "db/sanity_checks.sql",
        return_results=True
    )

    # Each SELECT in sanity_checks.sql corresponds to one rule
    # Any non-empty result set = violation
    for idx, result_set in enumerate(results, start=1):
        if not result_set.empty:
            raise ValueError(
                f"[BUSINESS SANITY ERROR] Rule #{idx} failed. "
                f"Rows found: {len(result_set)}"
            )

    print("[BUSINESS SANITY] All business-rule sanity checks passed.")
