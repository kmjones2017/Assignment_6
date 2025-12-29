from datetime import date, datetime
from prefect import flow, get_run_logger

from tasks.day9.run_context import show_run_context


def validate_run_date(run_date: str) -> str:
    """
    Ensures run_date is in YYYY-MM-DD format.
    Returns the same string if valid, otherwise raises a clear error.
    """
    try:
        datetime.strptime(run_date, "%Y-%m-%d")
        return run_date
    except ValueError:
        raise ValueError(f"run_date must be YYYY-MM-DD (example: 2025-12-28). You gave: {run_date}")


@flow(name="Day 9 - Scheduling and Parameters")
def day9_scheduling_flow(run_date: str | None = None):
    logger = get_run_logger()
    logger.info("=== Day 9 flow started ===")

    if run_date is None:
        run_date = date.today().isoformat()

    run_date = validate_run_date(run_date)

    logger.info(f"[Day9 Flow] run_date = {run_date}")
    show_run_context(run_date)

    logger.info("=== Day 9 flow finished ===")


if __name__ == "__main__":
    day9_scheduling_flow(run_date="2025-12-28")
