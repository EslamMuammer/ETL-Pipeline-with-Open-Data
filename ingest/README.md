# Stage 1: Data Ingestion and Inspection

## Dataset
Olist Brazilian E-Commerce Public Dataset
Source: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Tables Used (geolocation excluded)

| Table | File | Rows | Description |
|-------|------|------|-------------|
| orders | olist_orders_dataset.csv | 99,441 | Core orders data |
| customers | olist_customers_dataset.csv | 99,441 | Customer information |
| order_items | olist_order_items_dataset.csv | 112,650 | Items per order |
| payments | olist_order_payments_dataset.csv | 103,886 | Payment details |
| reviews | olist_order_reviews_dataset.csv | 99,224 | Customer reviews |
| products | olist_products_dataset.csv | 32,951 | Product information |
| sellers | olist_sellers_dataset.csv | 3,095 | Seller information |

## Notes

### category_translation
- Used to translate product category names from Portuguese to English
- English names merged into products table
- Role is complete and no longer needed

### Missing Values Detected
| Table | Column | Missing |
|-------|--------|---------|
| orders | order_approved_at | 160 |
| orders | order_delivered_carrier_date | 1,783 |
| orders | order_delivered_customer_date | 2,965 |
| reviews | review_comment_title | 87,656 |
| reviews | review_comment_message | 58,247 |
| products | product_category_name | 610 |
| products | product_weight_g | 2 |

## How to Run

1. Download dataset from Kaggle and place CSV files in data/raw/
2. Create virtual environment: python -m venv .venv
3. Install requirements: pip install pandas pyarrow
4. Run ingestion: python src/ingestion.py

## Output
- Parquet files saved in data/parquet/
- Ingestion log saved in logs/ingestion_log.txt
