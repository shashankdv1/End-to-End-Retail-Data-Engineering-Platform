# ============================================================
# transform.py
# PANDAS TRANSFORM LAYER
# ============================================================

from extract import extract_data
import pandas as pd


# ============================================================
# 1. TRANSFORM PRODUCT DATA
# ============================================================

def transform_products(df_products):

    print("\n========== PRODUCT TRANSFORMATION ==========")

    print("\n[DEBUG] Input Products:")
    print(df_products.head())

    print("\n[DEBUG] Input Shape:", df_products.shape)
    print("[DEBUG] Input Columns:", df_products.columns.tolist())

    df_products = df_products.copy()

    # --------------------------------------------------------
    # A. Handle Missing Values
    # --------------------------------------------------------

    df_products["title"] = df_products["title"].fillna("unknown")
    df_products["category"] = df_products["category"].fillna("unknown")
    df_products["base_price"] = df_products["base_price"].fillna(0)

    print("\n[DEBUG] After Missing Value Handling:")
    print(df_products.head())

    # --------------------------------------------------------
    # B. Data Type Casting
    # --------------------------------------------------------

    df_products["product_id"] = pd.to_numeric(
        df_products["product_id"],
        errors="coerce"
    )

    df_products["base_price"] = pd.to_numeric(
        df_products["base_price"],
        errors="coerce"
    )

    # --------------------------------------------------------
    # C. String Normalization
    # --------------------------------------------------------

    df_products["title"] = (
        df_products["title"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    df_products["category"] = (
        df_products["category"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # Remove special characters
    df_products["title"] = (
        df_products["title"]
        .str.replace(
            r"[^a-zA-Z0-9\s]",
            "",
            regex=True
        )
    )

    print("\n[DEBUG] After String Normalization:")
    print(df_products.head())

    # --------------------------------------------------------
    # D. Deduplication
    # --------------------------------------------------------

    df_products = df_products.drop_duplicates(
        subset=["product_id"]
    )

    print("\n[DEBUG] After Deduplication:")
    print("Shape:", df_products.shape)

    # --------------------------------------------------------
    # E. Business Logic - Price Classification
    # --------------------------------------------------------

    df_products["price_category"] = "Budget"

    df_products.loc[
        df_products["base_price"] >= 100,
        "price_category"
    ] = "Mid Range"

    df_products.loc[
        df_products["base_price"] >= 500,
        "price_category"
    ] = "Premium"

    print("\n[DEBUG] After Price Classification:")
    print(
        df_products[
            [
                "product_id",
                "base_price",
                "price_category"
            ]
        ].head(10)
    )

    # --------------------------------------------------------
    # F. Final Product Columns
    # --------------------------------------------------------

    df_products = df_products[
        [
            "product_id",
            "title",
            "category",
            "base_price",
            "price_category"
        ]
    ]

    print("\n[DEBUG] FINAL PRODUCTS:")
    print(df_products.head(10))

    return df_products


# ============================================================
# 2. TRANSFORM CLICKSTREAM DATA
# ============================================================

def transform_clicks(df_clicks):

    print("\n========== CLICKSTREAM TRANSFORMATION ==========")

    print("\n[DEBUG] Input Clicks:")
    print(df_clicks.head())

    print("\n[DEBUG] Input Shape:", df_clicks.shape)
    print("[DEBUG] Input Columns:", df_clicks.columns.tolist())

    df_clicks = df_clicks.copy()

    # --------------------------------------------------------
    # A. Handle Missing Values
    # --------------------------------------------------------

    df_clicks["action"] = df_clicks["action"].fillna("unknown")
    df_clicks["device"] = df_clicks["device"].fillna("unknown")
    df_clicks["user_id"] = df_clicks["user_id"].fillna("unknown")

    # --------------------------------------------------------
    # B. Data Type Casting
    # --------------------------------------------------------

    df_clicks["product_id"] = pd.to_numeric(
        df_clicks["product_id"],
        errors="coerce"
    )

    # --------------------------------------------------------
    # C. String Normalization
    # --------------------------------------------------------

    df_clicks["action"] = (
        df_clicks["action"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    df_clicks["device"] = (
        df_clicks["device"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # --------------------------------------------------------
    # D. Deduplication
    # --------------------------------------------------------

    df_clicks = df_clicks.drop_duplicates(
        subset=["session_id"]
    )

    print("\n[DEBUG] After Deduplication:")
    print("Shape:", df_clicks.shape)

    # --------------------------------------------------------
    # E. Conversion Flag
    # --------------------------------------------------------

    df_clicks["conversion_flag"] = (
        df_clicks["action"]
        .eq("add_to_cart")
        .astype(int)
    )

    # --------------------------------------------------------
    # F. Engagement Classification
    # --------------------------------------------------------

    df_clicks["engagement_type"] = "Unknown"

    df_clicks.loc[
        df_clicks["action"] == "view",
        "engagement_type"
    ] = "Low Intent"

    df_clicks.loc[
        df_clicks["action"] == "click",
        "engagement_type"
    ] = "Medium Intent"

    df_clicks.loc[
        df_clicks["action"] == "add_to_cart",
        "engagement_type"
    ] = "High Intent"

    print("\n[DEBUG] After Business Logic:")
    print(
        df_clicks[
            [
                "session_id",
                "product_id",
                "action",
                "conversion_flag",
                "engagement_type"
            ]
        ].head(10)
    )

    return df_clicks


# ============================================================
# 3. TRANSFORM SALES DATA
# ============================================================

def transform_sales(df_sales):

    print("\n========== SALES TRANSFORMATION ==========")

    print("\n[DEBUG] Input Sales:")
    print(df_sales.head())

    print("\n[DEBUG] Input Shape:", df_sales.shape)
    print("[DEBUG] Input Columns:", df_sales.columns.tolist())

    df_sales = df_sales.copy()

    # --------------------------------------------------------
    # A. Handle Missing Values
    # --------------------------------------------------------

    df_sales["quantity_sold"] = (
        df_sales["quantity_sold"]
        .fillna(0)
    )

    # --------------------------------------------------------
    # B. Data Type Casting
    # --------------------------------------------------------

    df_sales["product_id"] = pd.to_numeric(
        df_sales["product_id"],
        errors="coerce"
    )

    df_sales["quantity_sold"] = pd.to_numeric(
        df_sales["quantity_sold"],
        errors="coerce"
    )

    df_sales["date"] = pd.to_datetime(
        df_sales["date"],
        errors="coerce"
    )

    # --------------------------------------------------------
    # C. Deduplication
    # --------------------------------------------------------

    df_sales = df_sales.drop_duplicates(
        subset=["transaction_id"]
    )

    print("\n[DEBUG] After Deduplication:")
    print("Shape:", df_sales.shape)

    # --------------------------------------------------------
    # D. Date Transformations
    # --------------------------------------------------------

    df_sales["sale_year"] = (
        df_sales["date"].dt.year
    )

    df_sales["sale_month"] = (
        df_sales["date"].dt.month
    )

    df_sales["sale_day"] = (
        df_sales["date"].dt.day
    )

    # --------------------------------------------------------
    # E. Order Size Classification
    # --------------------------------------------------------

    df_sales["order_size"] = "Small"

    df_sales.loc[
        df_sales["quantity_sold"] >= 2,
        "order_size"
    ] = "Medium"

    df_sales.loc[
        df_sales["quantity_sold"] >= 4,
        "order_size"
    ] = "Large"

    print("\n[DEBUG] After Sales Transformations:")
    print(df_sales.head(10))

    return df_sales


# ============================================================
# 4. JOIN SALES WITH PRODUCTS
# ============================================================

def enrich_sales_with_products(
    df_sales,
    df_products
):

    print("\n========== SALES + PRODUCT JOIN ==========")

    df_sales_enriched = df_sales.merge(
        df_products,
        on="product_id",
        how="left"
    )

    print("\n[DEBUG] Sales Enriched:")
    print(df_sales_enriched.head(10))

    print(
        "\n[DEBUG] Shape:",
        df_sales_enriched.shape
    )

    return df_sales_enriched


# ============================================================
# 5. JOIN CLICKS WITH PRODUCTS
# ============================================================

def enrich_clicks_with_products(
    df_clicks,
    df_products
):

    print("\n========== CLICKSTREAM + PRODUCT JOIN ==========")

    df_clicks_enriched = df_clicks.merge(
        df_products,
        on="product_id",
        how="left"
    )

    print("\n[DEBUG] Clicks Enriched:")
    print(df_clicks_enriched.head(10))

    print(
        "\n[DEBUG] Shape:",
        df_clicks_enriched.shape
    )

    return df_clicks_enriched


# ============================================================
# 6. SALES AGGREGATION
# ============================================================

def aggregate_sales(df_sales_enriched):

    print("\n========== SALES AGGREGATION ==========")

    df_product_summary = (
        df_sales_enriched
        .groupby(
            [
                "product_id",
                "title",
                "category"
            ],
            dropna=False
        )
        .agg(
            total_units_sold=(
                "quantity_sold",
                "sum"
            ),

            transaction_count=(
                "transaction_id",
                "count"
            )
        )
        .reset_index()
    )

    print("\n[DEBUG] Product Summary:")
    print(df_product_summary.head(10))

    return df_product_summary


# ============================================================
# 7. CATEGORY AGGREGATION
# ============================================================

def aggregate_category_sales(
    df_sales_enriched
):

    print("\n========== CATEGORY AGGREGATION ==========")

    df_category_summary = (
        df_sales_enriched
        .groupby(
            "category",
            dropna=False
        )
        .agg(
            total_units_sold=(
                "quantity_sold",
                "sum"
            ),

            total_transactions=(
                "transaction_id",
                "count"
            )
        )
        .reset_index()
    )

    print("\n[DEBUG] Category Summary:")
    print(df_category_summary.head(10))

    return df_category_summary


# ============================================================
# 8. MONTHLY AGGREGATION
# ============================================================

def aggregate_monthly_sales(
    df_sales_enriched
):

    print("\n========== MONTHLY AGGREGATION ==========")

    df_monthly_sales = (
        df_sales_enriched
        .groupby(
            [
                "sale_year",
                "sale_month"
            ],
            dropna=False
        )
        .agg(
            units_sold=(
                "quantity_sold",
                "sum"
            ),

            total_transactions=(
                "transaction_id",
                "count"
            )
        )
        .reset_index()
        .sort_values(
            [
                "sale_year",
                "sale_month"
            ]
        )
    )

    print("\n[DEBUG] Monthly Sales:")
    print(df_monthly_sales.head(10))

    return df_monthly_sales


# ============================================================
# 9. MAIN TRANSFORM FUNCTION
# ============================================================

def transform_data(
    df_products,
    df_clicks,
    df_sales
):

    print("\n")
    print("=" * 60)
    print("STARTING TRANSFORMATION LAYER")
    print("=" * 60)

    # --------------------------------------------------------
    # Transform Individual DataFrames
    # --------------------------------------------------------

    df_products = transform_products(
        df_products
    )

    df_clicks = transform_clicks(
        df_clicks
    )

    df_sales = transform_sales(
        df_sales
    )

    # --------------------------------------------------------
    # Enrichment / Joins
    # --------------------------------------------------------

    df_sales_enriched = (
        enrich_sales_with_products(
            df_sales,
            df_products
        )
    )

    df_clicks_enriched = (
        enrich_clicks_with_products(
            df_clicks,
            df_products
        )
    )

    # --------------------------------------------------------
    # Aggregations
    # --------------------------------------------------------

    df_product_summary = (
        aggregate_sales(
            df_sales_enriched
        )
    )

    df_category_summary = (
        aggregate_category_sales(
            df_sales_enriched
        )
    )

    df_monthly_sales = (
        aggregate_monthly_sales(
            df_sales_enriched
        )
    )

    print("\n")
    print("=" * 60)
    print("TRANSFORMATION COMPLETED")
    print("=" * 60)

    # --------------------------------------------------------
    # Return all transformed DataFrames
    # --------------------------------------------------------

    return {
        "products": df_products,
        "clicks": df_clicks,
        "sales": df_sales,
        "sales_enriched": df_sales_enriched,
        "clicks_enriched": df_clicks_enriched,
        "product_summary": df_product_summary,
        "category_summary": df_category_summary,
        "monthly_sales": df_monthly_sales
    }


# ============================================================
# RUN EXTRACT + TRANSFORM
# ============================================================

df_products, df_clicks, df_sales = extract_data()

print("\n========== EXTRACT DATA TYPES ==========")
print("Products:", type(df_products))
print("Clicks:", type(df_clicks))
print("Sales:", type(df_sales))


result = transform_data(
    df_products,
    df_clicks,
    df_sales
)


# ============================================================
# FINAL DEBUG OUTPUT
# ============================================================

print("\n========== FINAL TRANSFORMED PRODUCTS ==========")
print(result["products"].head(10))

print("\n========== FINAL TRANSFORMED CLICKS ==========")
print(result["clicks"].head(10))

print("\n========== FINAL TRANSFORMED SALES ==========")
print(result["sales"].head(10))

print("\n========== SALES ENRICHED ==========")
print(result["sales_enriched"].head(10))

print("\n========== CLICKS ENRICHED ==========")
print(result["clicks_enriched"].head(10))

print("\n========== PRODUCT SUMMARY ==========")
print(result["product_summary"].head(10))

print("\n========== CATEGORY SUMMARY ==========")
print(result["category_summary"].head(10))

print("\n========== MONTHLY SALES ==========")
print(result["monthly_sales"].head(10))