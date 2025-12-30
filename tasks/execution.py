# -*- coding: utf-8 -*-
"""
Created on Tue Dec 30 12:47:11 2025

@author: pdraz
"""
from prefect import task, flow
import mysql.connector

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
# Task to execute a SQL file
# -----------------------------
@task
def execute_sql_file(file_path: str):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_commands = f.read()

        # MySQL allows multiple statements if multi=True
        for result in cursor.execute(sql_commands, multi=True):
            pass  # You can handle result if needed

        conn.commit()
        print(f"Executed SQL file: {file_path}")

    except Exception as e:
        conn.rollback()
        print(f"Error executing {file_path}: {e}")

    finally:
        cursor.close()
        conn.close()


# -----------------------------
# Example Flow
# -----------------------------
@flow
def run_sql_flow():
    execute_sql_file("path/to/your/sql_file.sql")


if _name_ == "_main_":
    run_sql_flow()

# remember to keep these lines where they are:
from dotenv import load_dotenv

load_dotenv()
