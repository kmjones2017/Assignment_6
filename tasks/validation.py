from prefect import task
import pandas as pd
import numpy as np


def _to_float_array(series: pd.Series) -> np.ndarray:
    """
    Normalize numeric Series (Decimal, int, float) to float ndarray
    for cross-engine numerical comparison.
    """
    return np.array(series, dtype=float)


@task
def validate_sql_vs_pandas(
    sql_results: dict[str, pd.DataFrame],
    pandas_results: dict[str, pd.DataFrame],
    tolerance: float = 0.01
):
    """
    Validates that SQL and Pandas analytics outputs match.
    Raises ValueError if validation fails.
    """

    # ============================
    # Revenue by Product Validation
    # ============================
    sql_rev = sql_results["revenue_by_product"].copy()
    pd_rev = pandas_results["revenue_by_product"].copy()

    sql_rev = sql_rev.sort_values("drug_id").reset_index(drop=True)
    pd_rev = pd_rev.sort_values("drug_id").reset_index(drop=True)

    # Structural assertions
    assert set(sql_rev.columns) == set(
        pd_rev.columns
    ), "Revenue by Product column sets do not match between SQL and Pandas"

    assert len(sql_rev) == len(
        pd_rev
    ), "Revenue by Product row counts differ between SQL and Pandas"

    # Key validation
    if not sql_rev["drug_id"].equals(pd_rev["drug_id"]):
        raise ValueError(
            "Mismatched drug_id column values between SQL and Pandas' "
            "Revenue by Product table!"
        )

    # Numeric validation
    for col in ["quantity_sold", "total_revenue"]:
        if not np.allclose(
            _to_float_array(sql_rev[col]),
            _to_float_array(pd_rev[col]),
            atol=tolerance
        ):
            raise ValueError(
                f"Mismatched {col} column values between SQL and Pandas' "
                "Revenue by Product table!"
            )

    print("[VALIDATION] Revenue by Product validated successfully")

    # ============================
    # Customer Segmentation Validation
    # ============================
    sql_cust = sql_results["customer_segmentation"].copy()
    pd_cust = pandas_results["customer_segmentation"].copy()

    sql_cust = sql_cust.sort_values("customer_id").reset_index(drop=True)
    pd_cust = pd_cust.sort_values("customer_id").reset_index(drop=True)

    # Structural assertions
    assert set(sql_cust.columns) == set(
        pd_cust.columns
    ), "Customer Segmentation column sets do not match between SQL and Pandas"

    assert len(sql_cust) == len(
        pd_cust
    ), "Customer Segmentation row counts differ between SQL and Pandas"

    # Key validation
    if not sql_cust["customer_id"].equals(pd_cust["customer_id"]):
        raise ValueError(
            "Mismatched customer_id column values between SQL and Pandas' "
            "Customer Segmentation table!"
        )

    # Numeric validation
    for col in ["total_spent", "order_count"]:
        if not np.allclose(
            _to_float_array(sql_cust[col]),
            _to_float_array(pd_cust[col]),
            atol=tolerance
        ):
            raise ValueError(
                f"Mismatched {col} column values between SQL and Pandas' "
                "Customer Segmentation table!"
            )

    # Categorical validation
    if not sql_cust["segment"].equals(pd_cust["segment"]):
        raise ValueError(
            "Mismatched segment column values between SQL and Pandas' "
            "Customer Segmentation table!"
        )

    print("[VALIDATION] Customer Segmentation validated successfully")
    print("[VALIDATION] All SQL vs Pandas checks passed")
