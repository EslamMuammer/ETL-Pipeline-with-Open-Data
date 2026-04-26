import pandas as pd
import os
import logging

# Setup
os.makedirs("data/processed", exist_ok=True)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/ingestion_log.txt",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

print("Stage 1 - Data Ingestion Started\n")
logging.info("Stage 1 Started")

# Tables (geolocation excluded)
tables = {
    "orders":               "data/raw/olist_orders_dataset.csv",
    "customers":            "data/raw/olist_customers_dataset.csv",
    "order_items":          "data/raw/olist_order_items_dataset.csv",
    "payments":             "data/raw/olist_order_payments_dataset.csv",
    "reviews":              "data/raw/olist_order_reviews_dataset.csv",
    "products":             "data/raw/olist_products_dataset.csv",
    "sellers":              "data/raw/olist_sellers_dataset.csv",
    "category_translation": "data/raw/product_category_name_translation.csv",
}

# Datetime Columns
datetime_cols = {
    "orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ],
    "reviews":     ["review_creation_date", "review_answer_timestamp"],
    "order_items": ["shipping_limit_date"],
}

# Ingestion Loop
for name, path in tables.items():
    print("=" * 50)
    print(f"Loading: {name}")

    df = pd.read_csv(path)
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")

    for col in datetime_cols.get(name, []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    print(f"   Rows       : {len(df):,}")
    print(f"   Columns    : {len(df.columns)}")
    print(f"   Col Names  : {list(df.columns)}")

    missing = df.isnull().sum()
    missing = missing[missing > 0]
    if len(missing) > 0:
        print(f"   Missing    :\n{missing}")
    else:
        print(f"   Missing    : None")

    dups = df.duplicated().sum()
    if dups > 0:
        print(f"   Duplicates : {dups}")
    else:
        print(f"   Duplicates : None")

    # Merge category_translation into products
    if name == "products":
        print("\n   Merging category_translation into products...")

        category_df = pd.read_csv("data/raw/product_category_name_translation.csv")
        category_df.columns = category_df.columns.str.lower().str.strip()

        df = df.merge(category_df, on="product_category_name", how="left")

        df["product_category_name"] = df["product_category_name_english"].fillna(df["product_category_name"])

        df.drop(columns=["product_category_name_english"], inplace=True)

        print(f"   Category names merged successfully")
        print(f"   Columns after merge : {list(df.columns)}")

        logging.info("products: category_translation merged, category names now in English")

    # Save as CSV
    out = f"data/processed/{name}.csv"

    if name == "category_translation":
        print(f"   Note: category_translation merged into products - role complete")
        logging.info("category_translation: role complete after merge into products")

    df.to_csv(out, index=False)
    print(f"   Saved CSV -> {out}")

    logging.info(f"{name} | {len(df):,} rows | {len(df.columns)} cols | missing: {missing.to_dict()} | dups: {dups}")

print("=" * 50)
print("Stage 1 Complete! Check logs/ingestion_log.txt")
logging.info("Stage 1 Completed Successfully")
