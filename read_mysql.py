from prefect import task
import pandas as pd
from sqlalchemy import create_engine
from config import db_config

@task(name="Read Table From MySQL")
def read_table(table_name: str) -> pd.DataFrame:
    # Using mysql-connector driver
    user = db_config["user"]
    password = db_config["password"]
    host = db_config["host"]
    database = db_config["database"]

    engine = create_engine(
        f"mysql+mysqlconnector://{user}:{password}@{host}:3306/{database}"
    )

    df = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)
    print(f"✅ Read {len(df)} rows from {table_name}")
    return df
