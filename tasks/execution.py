# -*- coding: utf-8 -*-
"""
Created on Tue Dec 30 12:47:11 2025

@author: pdraz
"""

import os
import mysql.connector
from dotenv import load_dotenv
from prefect import task, flow

load_dotenv()

# -------------------------
# Database connection
# -------------------------
@task
def get_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
    )
