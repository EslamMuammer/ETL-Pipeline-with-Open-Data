"""
Stage 5 – Visualization
========================
Generates exploratory charts for the Olist Brazilian E-Commerce dataset
using Pandas and Plotly.

Assumptions
-----------
- The raw CSV files live one level up from this script in a `data/` folder:
      visualization/
          visualize.py   ← this file
      data/
          olist_customers_dataset.csv
          olist_orders_dataset.csv
          olist_order_items_dataset.csv
          olist_order_payments_dataset.csv
          olist_products_dataset.csv
          olist_geolocation_dataset.csv
          product_category_name_translation.csv

  Change DATA_DIR below if your layout differs.

- Charts are saved as standalone HTML files inside visualization/charts/
  (created automatically) AND displayed interactively in the browser.

Dependencies
------------
    pip install pandas plotly
"""

import os
import pathlib

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR = pathlib.Path(__file__).parent          # visualization/
DATA_DIR   = SCRIPT_DIR.parent / "ingest" / "data" / "csv"   # ../ingest/data/csv/
OUTPUT_DIR = SCRIPT_DIR / "charts"
OUTPUT_DIR.mkdir(exist_ok=True)


def save(fig: go.Figure, filename: str) -> None:
    """Save a Plotly figure as an HTML file and show it."""
    path = OUTPUT_DIR / filename
    fig.write_html(path)
    print(f"  ✓ Saved → {path}")
    fig.show()


# ── Load Data ──────────────────────────────────────────────────────────────────
print("Loading datasets …")

customers  = pd.read_csv(DATA_DIR / "customers.csv")
orders     = pd.read_csv(DATA_DIR / "orders.csv",
                         parse_dates=["order_purchase_timestamp",
                                      "order_delivered_customer_date",
                                      "order_estimated_delivery_date"])
items      = pd.read_csv(DATA_DIR / "order_items.csv")
payments   = pd.read_csv(DATA_DIR / "payments.csv")
products   = pd.read_csv(DATA_DIR / "products.csv")
categories = pd.read_csv(DATA_DIR / "category_translation.csv")
sellers    = pd.read_csv(DATA_DIR / "sellers.csv")

print(f"  customers : {len(customers):,} rows")
print(f"  orders    : {len(orders):,} rows")
print(f"  items     : {len(items):,} rows")
print(f"  payments  : {len(payments):,} rows")
print(f"  sellers   : {len(sellers):,} rows")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 1 – Top 15 Cities by Number of Customers
# ══════════════════════════════════════════════════════════════════════════════
print("\n[1/6] Top cities by customer count …")

top_cities = (
    customers["customer_city"]
    .str.title()
    .value_counts()
    .head(15)
    .reset_index()
    .rename(columns={"index": "city", "customer_city": "city",
                     "count": "customers"})
)
# pandas ≥ 2.0 uses "count" automatically; rename defensively
top_cities.columns = ["city", "customers"]

fig1 = px.bar(
    top_cities.sort_values("customers"),
    x="customers",
    y="city",
    orientation="h",
    title="Top 15 Cities by Number of Customers",
    labels={"customers": "Number of Customers", "city": "City"},
    color="customers",
    color_continuous_scale="Blues",
    text="customers",
)
fig1.update_traces(textposition="outside")
fig1.update_layout(coloraxis_showscale=False, yaxis_title="", height=500)
save(fig1, "01_top_cities_customers.html")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 2 – Monthly Order Volume & Revenue Over Time
# ══════════════════════════════════════════════════════════════════════════════
print("[2/6] Monthly order volume & revenue …")

# Join orders ← items to get price, then merge with order date
order_revenue = (
    items.groupby("order_id")["price"]
    .sum()
    .reset_index()
    .rename(columns={"price": "revenue"})
)
orders_enriched = orders.merge(order_revenue, on="order_id", how="left")
orders_enriched["month"] = orders_enriched["order_purchase_timestamp"].dt.to_period("M")

monthly = (
    orders_enriched.groupby("month")
    .agg(order_count=("order_id", "count"), revenue=("revenue", "sum"))
    .reset_index()
)
monthly["month"] = monthly["month"].dt.to_timestamp()

fig2 = make_subplots(specs=[[{"secondary_y": True}]])
fig2.add_trace(
    go.Bar(x=monthly["month"], y=monthly["order_count"],
           name="Order Count", marker_color="#4A90D9", opacity=0.7),
    secondary_y=False,
)
fig2.add_trace(
    go.Scatter(x=monthly["month"], y=monthly["revenue"],
               name="Revenue (BRL)", mode="lines+markers",
               line=dict(color="#E07B39", width=2)),
    secondary_y=True,
)
fig2.update_layout(
    title="Monthly Order Volume & Revenue",
    xaxis_title="Month",
    legend=dict(x=0.01, y=0.99),
    hovermode="x unified",
)
fig2.update_yaxes(title_text="Order Count", secondary_y=False)
fig2.update_yaxes(title_text="Revenue (BRL)", secondary_y=True)
save(fig2, "02_monthly_orders_revenue.html")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 3 – Payment Type Distribution
# ══════════════════════════════════════════════════════════════════════════════
print("[3/6] Payment type distribution …")

payment_dist = (
    payments["payment_type"]
    .str.replace("_", " ")
    .str.title()
    .value_counts()
    .reset_index()
)
payment_dist.columns = ["payment_type", "count"]

fig3 = px.pie(
    payment_dist,
    names="payment_type",
    values="count",
    title="Payment Type Distribution",
    color_discrete_sequence=px.colors.qualitative.Pastel,
    hole=0.4,
)
fig3.update_traces(textposition="outside", textinfo="percent+label")
save(fig3, "03_payment_types.html")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 4 – Top 15 Product Categories by Revenue
# ══════════════════════════════════════════════════════════════════════════════
print("[4/6] Top product categories by revenue …")

# Translate category names to English
products_en = products.merge(
    categories, on="product_category_name", how="left"
)
items_cat = items.merge(
    products_en[["product_id", "product_category_name_english"]],
    on="product_id",
    how="left",
)
cat_revenue = (
    items_cat.groupby("product_category_name_english")["price"]
    .sum()
    .reset_index()
    .rename(columns={"product_category_name_english": "category",
                     "price": "revenue"})
    .dropna(subset=["category"])
    .sort_values("revenue", ascending=False)
    .head(15)
)
cat_revenue["category"] = cat_revenue["category"].str.replace("_", " ").str.title()

fig4 = px.bar(
    cat_revenue.sort_values("revenue"),
    x="revenue",
    y="category",
    orientation="h",
    title="Top 15 Product Categories by Revenue (BRL)",
    labels={"revenue": "Total Revenue (BRL)", "category": "Category"},
    color="revenue",
    color_continuous_scale="Greens",
    text=cat_revenue.sort_values("revenue")["revenue"].apply(
        lambda v: f"R$ {v:,.0f}"
    ),
)
fig4.update_traces(textposition="outside")
fig4.update_layout(coloraxis_showscale=False, yaxis_title="", height=550)
save(fig4, "04_top_categories_revenue.html")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 5 – Order Status Breakdown
# ══════════════════════════════════════════════════════════════════════════════
print("[5/6] Order status breakdown …")

status_counts = (
    orders["order_status"]
    .str.replace("_", " ")
    .str.title()
    .value_counts()
    .reset_index()
)
status_counts.columns = ["status", "count"]

fig5 = px.bar(
    status_counts,
    x="status",
    y="count",
    title="Order Status Breakdown",
    labels={"status": "Order Status", "count": "Number of Orders"},
    color="status",
    color_discrete_sequence=px.colors.qualitative.Set2,
    text="count",
)
fig5.update_traces(textposition="outside", showlegend=False)
fig5.update_layout(xaxis_title="", height=450)
save(fig5, "05_order_status.html")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 6 – Average Review Score by Product Category
# ══════════════════════════════════════════════════════════════════════════════
print("[6/6] Average review score by product category …")

reviews = pd.read_csv(DATA_DIR / "reviews.csv")

# Join items → products → English category names → reviews
items_products = items.merge(
    products[["product_id", "product_category_name"]], on="product_id", how="left"
).merge(categories, on="product_category_name", how="left")

items_reviews = items_products.merge(
    reviews[["order_id", "review_score"]], on="order_id", how="left"
)

cat_scores = (
    items_reviews.groupby("product_category_name_english")["review_score"]
    .agg(avg_score="mean", review_count="count")
    .reset_index()
    .dropna(subset=["product_category_name_english"])
    .query("review_count >= 50")   # only categories with enough reviews
    .sort_values("avg_score", ascending=False)
    .head(20)
)
cat_scores["product_category_name_english"] = (
    cat_scores["product_category_name_english"].str.replace("_", " ").str.title()
)

fig6 = px.bar(
    cat_scores.sort_values("avg_score"),
    x="avg_score",
    y="product_category_name_english",
    orientation="h",
    title="Top 20 Product Categories by Average Review Score (min. 50 reviews)",
    labels={"avg_score": "Avg Review Score (1–5)",
            "product_category_name_english": "Category"},
    color="avg_score",
    color_continuous_scale="RdYlGn",
    text=cat_scores.sort_values("avg_score")["avg_score"].apply(lambda v: f"{v:.2f}"),
)
fig6.update_traces(textposition="outside")
fig6.update_layout(coloraxis_showscale=False, yaxis_title="", height=600)
save(fig6, "06_avg_review_score_by_category.html")


print(f"\n✅  All 6 charts saved to: {OUTPUT_DIR}")