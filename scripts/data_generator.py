import pandas as pd
import random
import time
import os
from datetime import datetime

# --- Configuration ---
DATA_DIR = "/opt/airflow/data"
SHIPMENTS_FILE = os.path.join(DATA_DIR, "incoming_shipments.csv")
ORDERS_FILE = os.path.join(DATA_DIR, "new_orders.csv")
# HEARTBEAT_FILE = os.path.join(DATA_DIR, "generator.heartbeat")

FLOWER_TYPES = ["Rose", "Tulip", "Daisy", "Sunflower", "Orchid"]
# --------------------

# Initialize counters to avoid ID collisions if you restart the script
shipment_id_counter = 100
order_id_counter = 900

print(" Starting data generator...")

while True:
    # Update the heartbeat file to show the generator is alive
    # with open(HEARTBEAT_FILE, "w") as f:
        # f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) # Add this line
    # Randomly decide whether to generate a shipment or an order
    if random.random() > 0.5:
        # --- Generate a new shipment ---
        shipment_id_counter += 1
        flower = random.choice(FLOWER_TYPES)
        quantity = random.randint(50, 200)
        arrival_date = datetime.now().strftime('%Y-%m-%d')
        
        new_shipment = pd.DataFrame([{
            'shipment_id': f'SH{shipment_id_counter}',
            'flower_type': flower,
            'quantity': quantity,
            'arrival_date': arrival_date
        }])
        
        # Append the new data to the CSV file
        new_shipment.to_csv(SHIPMENTS_FILE, mode='a', header=False, index=False)
        print(f"📦 Added Shipment: {quantity} {flower} (ID: SH{shipment_id_counter})")

    else:
        # --- Generate a new order ---
        order_id_counter += 1
        flower = random.choice(FLOWER_TYPES)
        quantity = random.randint(10, 50)
        
        new_order = pd.DataFrame([{
            'order_id': f'ORD{order_id_counter}',
            'flower_type': flower,
            'quantity': quantity
        }])
        
        # Append the new data to the CSV file
        new_order.to_csv(ORDERS_FILE, mode='a', header=False, index=False)
        print(f"🛒 Added Order: {quantity} {flower} (ID: ORD{order_id_counter})")

    # Wait for a random interval before the next run
    sleep_time = random.randint(1,10)
    print(f"--- Waiting for {sleep_time} seconds... ---")
    time.sleep(sleep_time)