from prefect import flow
from tasks.read_mysql import read_table
from tasks.merge_data import merge_data
from tasks.write_mysql import write_dataframe

@flow(name="Day 7 - Merge and Load to MySQL")
def day7_flow():
    customers = read_table("customers")
    orders = read_table("orders")
    order_items = read_table("order_items")
    drug_batches = read_table("drug_batches")
    drugs = read_table("drugs")

    merged = merge_data(customers, orders, order_items, drug_batches, drugs)

    # Load merged result into analytics table
    write_dataframe(merged, "fact_orders_enriched")

if __name__ == "__main__":
    day7_flow()
