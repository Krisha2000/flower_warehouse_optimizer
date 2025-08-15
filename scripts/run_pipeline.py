import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from pathlib import Path
import shutil
import time
import os

# --- Database Connection ---
db_uri = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
engine = create_engine(db_uri)

# --- File Paths ---
DATA_DIR = Path("/opt/airflow/data")
SHIPMENTS_FILE = DATA_DIR / "incoming_shipments.csv"
ORDERS_FILE = DATA_DIR / "new_orders.csv"

shipment_cols = ['shipment_id', 'flower_type', 'quantity', 'arrival_date']
order_cols = ['order_id', 'flower_type', 'quantity']

# --- Helper function to safely copy file ---
def safe_copy(src_path: Path, prefix: str):
    if not src_path.exists() or src_path.stat().st_size == 0:
        return pd.DataFrame(columns=shipment_cols if prefix == "shipments" else order_cols)
    run_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    dest_path = DATA_DIR / f"processing_{prefix}_{run_timestamp}.csv"
    shutil.copyfile(src_path, dest_path)
    open(src_path, "w").close()  # truncate the source file
    if prefix == "shipments":
        return pd.read_csv(dest_path, header=None, names=shipment_cols)
    else:
        return pd.read_csv(dest_path, header=None, names=order_cols)

# --- Load new data ---
df_shipments = safe_copy(SHIPMENTS_FILE, "shipments")
df_orders = safe_copy(ORDERS_FILE, "orders")

# --- ELT: Load staging tables ---
df_shipments.to_sql('staging_shipments', engine, if_exists='replace', index=False)
df_orders.to_sql('staging_orders', engine, if_exists='replace', index=False)
print(f"ELT part: Loaded {len(df_shipments)} new shipments and {len(df_orders)} new orders to staging.")

# --- ETL: Optimization Logic ---
df_staged_shipments = pd.read_sql("SELECT * FROM staging_shipments", engine)
df_staged_orders = pd.read_sql("SELECT * FROM staging_orders", engine)
df_bins = pd.read_sql("SELECT * FROM warehouse_bins", engine)
df_catalog = pd.read_sql("SELECT * FROM flower_catalog", engine)
df_inventory = pd.read_sql("SELECT * FROM inventory", engine)

new_tasks = []

# Inbound Optimization
for _, shipment in df_staged_shipments.iterrows():
    if shipment['shipment_id'] not in df_inventory['shipment_id'].values:
        flower_info_df = df_catalog[df_catalog['flower_type'] == shipment['flower_type']]
        if not flower_info_df.empty:
            flower_info = flower_info_df.iloc[0]
            possible_bins = df_bins[df_bins['temperature_celsius'] == flower_info['required_temp_celsius']]
            occupied_bins = df_inventory['bin_id'].unique()
            available_bins = possible_bins[~possible_bins['bin_id'].isin(occupied_bins)]
            if not available_bins.empty:
                chosen_bin = available_bins.sort_values('distance_m').iloc[0]
                task_detail = f"Store {shipment['quantity']} {shipment['flower_type']} in Bin {chosen_bin['bin_id']}"
                new_tasks.append({'task_type': 'STORAGE', 'details': task_detail, 'timestamp': datetime.now()})
                new_inventory_record = pd.DataFrame([{
                    'shipment_id': shipment['shipment_id'],
                    'flower_type': shipment['flower_type'],
                    'quantity': shipment['quantity'],
                    'arrival_date': shipment['arrival_date'],
                    'bin_id': chosen_bin['bin_id']
                }])
                df_inventory = pd.concat([df_inventory, new_inventory_record], ignore_index=True)
        else:
            print(f"⚠️ Warning: Flower type '{shipment['flower_type']}' not found in catalog. Skipping.")

# Outbound Optimization
for _, order in df_staged_orders.iterrows():
    stock_for_order = df_inventory[df_inventory['flower_type'] == order['flower_type']].copy()
    if not stock_for_order.empty and stock_for_order['quantity'].sum() >= order['quantity']:
        stock_for_order['arrival_date'] = pd.to_datetime(stock_for_order['arrival_date'])
        stock_for_order = stock_for_order.sort_values('arrival_date')
        qty_to_fulfill = order['quantity']
        for index, stock_batch in stock_for_order.iterrows():
            if qty_to_fulfill > 0:
                pick_qty = min(qty_to_fulfill, stock_batch['quantity'])
                task_detail = f"For Order {order['order_id']}, Pick {pick_qty} from Bin {stock_batch['bin_id']}"
                new_tasks.append({'task_type': 'PICKING', 'details': task_detail, 'timestamp': datetime.now()})
                df_inventory.loc[df_inventory.index == index, 'quantity'] -= pick_qty
                qty_to_fulfill -= pick_qty

# Load results
if new_tasks:
    pd.DataFrame(new_tasks).to_sql('tasks', engine, if_exists='append', index=False)

df_inventory = df_inventory[df_inventory['quantity'] > 0]
df_inventory.to_sql('inventory', engine, if_exists='replace', index=False)

print("ETL part: Optimization complete.")
