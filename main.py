import os
import logging
import pandas as pd
import yaml
from sqlalchemy import create_engine, text
import psycopg2
from logging.handlers import RotatingFileHandler

# === 1. Load config ===
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

DB_URL = config["database"]["db_url"]
PG_RAW = config["database"]["pg_raw"]
DATA_DIR = config["data"]["directory"]
LANDING_TABLE = config["data"]["landing_table"]
BULK_TABLE = config["data"]["bulk_table"]
ANALYTICS_TABLE = config["data"]["analytics_table"]
LOG_FILE = config["logging"]["file"]
LOG_LEVEL = config["logging"]["level"]

# === 2. Setup Logging ===
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL.upper()))

# Console Handler
console_handler = logging.StreamHandler()
console_handler.setLevel(getattr(logging, LOG_LEVEL.upper()))

# File Handler
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=1000000, backupCount=3)
file_handler.setLevel(getattr(logging, LOG_LEVEL.upper()))

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

# === 3. Load data files ===
all_files = [
    os.path.join(DATA_DIR, f)
    for f in os.listdir(DATA_DIR)
    if f.endswith('.csv')
]

engine = create_engine(DB_URL)

with engine.connect() as conn:
    result = conn.execute(text(f"SELECT MAX(order_date) FROM {LANDING_TABLE}"))
    latest_date = result.scalar()
    logger.info(f"Latest order_date in {LANDING_TABLE}: {latest_date}")

df_list = []

for file in all_files:
    try:
        temp_df = pd.read_csv(file)
        logger.info(f"Data loaded from {file}")
    except Exception as e:
        logger.error(f"Error loading {file}: {e}")
        continue

    temp_df['order_date'] = pd.to_datetime(temp_df['order_date'])

    # Filter only new records
    if latest_date:
        temp_df = temp_df[temp_df['order_date'] > latest_date]

    temp_df.dropna(subset=['customer_id', 'amount'], inplace=True)

    if not temp_df.empty:
        df_list.append(temp_df)

if df_list:
    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df.to_sql(BULK_TABLE, engine, if_exists='append', index=False)
    logger.info(f"{len(combined_df)} new rows written to {BULK_TABLE}")
else:
    logger.info("No new data to load.")

# === 4. Create Analytics Table ===
try:
    conn = psycopg2.connect(**PG_RAW)
    cur = conn.cursor()

    cur.execute(f"""
        DROP TABLE IF EXISTS {ANALYTICS_TABLE};
        CREATE TABLE {ANALYTICS_TABLE} AS
        SELECT
            customer_id,
            COUNT(order_id) AS total_orders,
            ROUND(SUM(amount)::NUMERIC, 2) AS total_spent
        FROM {LANDING_TABLE}
        WHERE amount > 0
        GROUP BY customer_id;
    """)

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Analytics table {ANALYTICS_TABLE} created successfully.")
except Exception as e:
    logger.error(f"Failed to create analytics table: {e}")