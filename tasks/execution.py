# -*- coding: utf-8 -*-
"""
Created on Tue Dec 30 12:47:11 2025

@author: pdraz
"""

from prefect import flow, task
import mysql.connector
import pandas as pd

# -----------------------------
# MySQL Connection Helper
# -----------------------------
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="pharma_db",
        allow_local_infile=True
    )

# -----------------------------
# INSERT TASK (Batch Insert)
# -----------------------------
@task
def insert_batch(table: str, df: pd.DataFrame):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        conn.start_transaction()

        cols = ",".join(df.columns)
        placeholders = ",".join(["%s"] * len(df.columns))
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

        cursor.executemany(sql, df.to_records(index=False))
        conn.commit()

        print(f"Inserted {len(df)} rows into {table}")

    except Exception as e:
        print("Insert failed:", e)
        conn.rollback()
        print("Transaction rolled back")

    finally:
        cursor.close()
        conn.close()

# -----------------------------
# UPDATE TASK
# -----------------------------
@task
def update_customer_email(customer_id: int, new_email: str):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        conn.start_transaction()

        sql = """
        UPDATE customers
        SET email = %s
        WHERE customer_id = %s
        """

        cursor.execute(sql, (new_email, customer_id))
        conn.commit()

        print(f"Updated email for customer {customer_id}")

    except Exception as e:
        print("Update failed:", e)
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

# -----------------------------
# AGGREGATION TASK
# -----------------------------
@task
def get_total_spent_by_customer_type():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    sql = """
    SELECT 
        c.customer_type,
        SUM(oi.quantity * oi.unit_price) AS total_spent
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY c.customer_type;
    """

    cursor.execute(sql)
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    print("\nTotal Spent by Customer Type:")
    for row in results:
        print(row)

    return results

# -----------------------------
# MAIN FLOW
# -----------------------------
@flow
def pharma_etl_flow():

    # Example: Load CSVs into DataFrames
    customers_df = pd.read_csv("C:/Users/pdraz/pharma_project/data/customers.csv")
    orders_df = pd.read_csv("C:/Users/pdraz/pharma_project/data/orders.csv")
    order_items_df = pd.read_csv("C:/Users/pdraz/pharma_project/data/order_items.csv")

    # 1. Batch Inserts
    insert_batch("customers", customers_df)
    insert_batch("orders", orders_df)
    insert_batch("order_items", order_items_df)

    # 2. Update Example
    update_customer_email(1, "updated_email@example.com")

    # 3. Aggregation Example
    get_total_spent_by_customer_type()


if __name__ == "__main__":
    pharma_etl_flow()
