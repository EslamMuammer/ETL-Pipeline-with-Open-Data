/*******************************************************************************
OLIST DATA WAREHOUSE - STAGE 3: STAR SCHEMA & DATA INTEGRITY
Author: [Adham Mohammed Salah]
Description: This script transforms raw staging data into an optimized Star Schema.
*******************************************************************************/

-- =============================================
-- 2. CREATE DIMENSION TABLES
-- =============================================
-- Creating Customer Dimension
SELECT 
    customer_id, 
    customer_unique_id, 
    customer_city, 
    customer_state
INTO dim_customers
FROM olist_customers_dataset;

-- Creating Product Dimension with English Translations
SELECT 
    p.product_id,
    t.product_category_name_english AS product_category,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
INTO dim_products
FROM olist_products_dataset p
LEFT JOIN product_category_name_translation t 
    ON p.product_category_name = t.product_category_name;

-- =============================================
-- 3. CREATE FACT TABLE
-- =============================================
SELECT 
    i.order_id,
    i.product_id,
    i.seller_id,
    o.customer_id,
    o.order_purchase_timestamp,
    o.order_status,
    i.price,
    i.freight_value,
    (i.price + i.freight_value) AS total_order_value
INTO fact_sales
FROM olist_order_items_dataset i
JOIN olist_orders_dataset o ON i.order_id = o.order_id;

-- =============================================
-- 5. VALIDATION CHECKS
-- =============================================
SELECT COUNT(*) AS Raw_Items FROM olist_order_items_dataset;
SELECT COUNT(*) AS Fact_Items FROM fact_sales;

SELECT COUNT(*) FROM dim_products WHERE product_category IS NULL;

SELECT COUNT(*) AS Orphaned_Sales
FROM fact_sales f
LEFT JOIN dim_customers c ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- =============================================
-- 6. ANALYTICAL DEMO
-- =============================================
-- Revenue by Customer State
SELECT 
    c.customer_state,
    ROUND(SUM(f.price), 2) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS total_orders
FROM fact_sales f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY total_revenue DESC;

-- Final View of Star Schema Relationships
SELECT 
    f.order_id,
    c.customer_city,     -- From dim_customers
    p.product_category,  -- From dim_products
    f.price,             -- From fact_sales
    f.freight_value      -- From fact_sales
FROM fact_sales f
JOIN dim_customers c ON f.customer_id = c.customer_id
JOIN dim_products p ON f.product_id = p.product_id;

-- =============================================
-- 4. DATA INTEGRITY CONSTRAINTS
-- This ensures the Star Schema relations are enforced
-- =============================================
-- Set Primary Keys
ALTER TABLE dim_customers ALTER COLUMN customer_id NVARCHAR(50) NOT NULL;
ALTER TABLE dim_customers ADD CONSTRAINT PK_customers PRIMARY KEY (customer_id);
ALTER TABLE dim_products ALTER COLUMN product_id NVARCHAR(50) NOT NULL;
ALTER TABLE dim_products ADD CONSTRAINT PK_products PRIMARY KEY (product_id);

-- Set Foreign Keys (Links the Fact table to Dimensions)
ALTER TABLE fact_sales ALTER COLUMN customer_id NVARCHAR(50) NOT NULL;
ALTER TABLE fact_sales ADD CONSTRAINT FK_sales_customers 
FOREIGN KEY (customer_id) REFERENCES dim_customers (customer_id);
ALTER TABLE fact_sales ALTER COLUMN product_id NVARCHAR(50) NOT NULL;
ALTER TABLE fact_sales ADD CONSTRAINT FK_sales_products 
FOREIGN KEY (product_id) REFERENCES dim_products (product_id);