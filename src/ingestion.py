import pandas as pd
import os
import logging

# ─── Setup ───────────────────────────────────────────────────
os.makedirs("data/parquet", exist_ok=True)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/ingestion_log.txt",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

print("🚀 Stage 1 — Data Ingestion Started\n")
logging.info("Stage 1 Started")

# ─── Tables ───────────────────────────────
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

# ─── Datetime Columns ─────────────────────────────────────────
datetime_cols = {
    "orders":      [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ],
    "reviews":     ["review_creation_date", "review_answer_timestamp"],
    "order_items": ["shipping_limit_date"],
}

# ─── Ingestion Loop ───────────────────────────────────────────
for name, path in tables.items():
    print(f"{'='*50}")
    print(f"📥 Loading: {name}")

    # 1. Load CSV
    df = pd.read_csv(path)

    # 2. Standardize Column Names
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")

    # 3. Convert Datetime Columns
    for col in datetime_cols.get(name, []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # 4. Inspect
    print(f"   ✅ Rows       : {len(df):,}")
    print(f"   ✅ Columns    : {len(df.columns)}")
    print(f"   ✅ Col Names  : {list(df.columns)}")

    missing = df.isnull().sum()
    missing = missing[missing > 0]
    if len(missing) > 0:
        print(f"   ⚠️  Missing   :\n{missing}")
    else:
        print(f"   ✅ Missing    : None")

    dups = df.duplicated().sum()
    if dups > 0:
        print(f"   ⚠️  Duplicates: {dups}")
    else:
        print(f"   ✅ Duplicates : None")

    # 5. Save as Parquet
    out = f"data/parquet/{name}.parquet"
    df.to_parquet(out, index=False, engine="pyarrow")
    print(f"   💾 Saved Parquet → {out}")

    logging.info(f"{name} | {len(df):,} rows | {len(df.columns)} cols | missing: {missing.to_dict()} | dups: {dups}")

print(f"\n{'='*50}")
print("🎉 Stage 1 Complete! Check logs/ingestion_log.txt")
logging.info("Stage 1 Completed Successfully")