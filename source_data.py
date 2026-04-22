import os
import pandas as pd
from sqlalchemy import create_engine, text

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "tp_ddev_1")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin123")

ARCHIVE_PATH = os.getenv("ARCHIVE_PATH", r"C:\Users\liamm\Downloads\archive")

OLIST_TABLES = {
    "customers":                   "olist_customers_dataset.csv",
    "geolocation":                 "olist_geolocation_dataset.csv",
    "order_items":                 "olist_order_items_dataset.csv",
    "order_payments":              "olist_order_payments_dataset.csv",
    "order_reviews":               "olist_order_reviews_dataset.csv",
    "orders":                      "olist_orders_dataset.csv",
    "products":                    "olist_products_dataset.csv",
    "sellers":                     "olist_sellers_dataset.csv",
    "product_category_translation":"product_category_name_translation.csv",
}

def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)

def create_schemas(engine):
    schemas = ["source_data", "data_warehouse", "raw_zone", "curated_zone","processed_zone" , "data_lake"]
    with engine.begin() as conn:
        for schema in schemas:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            print(f"  schema '{schema}' ready")

def load_olist(engine):
    for table_name, filename in OLIST_TABLES.items():
        filepath = os.path.join(ARCHIVE_PATH, filename)
        df = pd.read_csv(filepath)
        df.to_sql(
            table_name,
            engine,
            schema="source_data",
            if_exists="replace",
            index=False,
        )
        print(f"  source_data.{table_name}: {len(df)} rows loaded")

def init_database():
    try:
        engine = get_engine()
        print("Creating schemas...")
        create_schemas(engine)
        print("Loading Olist dataset into source_data...")
        load_olist(engine)
        print("✅ Done.")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    init_database()
