import pandas as pd
from sqlalchemy import create_engine

# --- FINAL FIX: Build the connection string manually ---
# This connects directly to the postgres service defined in docker-compose.yaml
db_uri = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
engine = create_engine(db_uri)
# ---------------------------------------------------------

# Load static data from our CSVs
df_layout = pd.read_csv('/opt/airflow/data/warehouse_layout.csv')
df_catalog = pd.read_csv('/opt/airflow/data/flower_catalog.csv')

# Create and populate the static tables
df_layout.to_sql('warehouse_bins', engine, if_exists='replace', index=False)
df_catalog.to_sql('flower_catalog', engine, if_exists='replace', index=False)

# Create the empty tables that will hold our dynamic data
pd.DataFrame(columns=['shipment_id', 'flower_type', 'quantity', 'arrival_date', 'bin_id']).to_sql('inventory', engine, if_exists='replace', index=False)
pd.DataFrame(columns=['task_id', 'task_type', 'details', 'timestamp']).to_sql('tasks', engine, if_exists='replace', index=False)

print("Database tables initialized successfully.")