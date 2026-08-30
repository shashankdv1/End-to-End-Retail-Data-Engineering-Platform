# ============================================================
# PySpark TRANSFORM Layer
# ============================================================
from extract import extract_data
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

df_products,df_clicks,df_sales = extract_data
# ============================================================
# 1. TRANSFORM PRODUCT DATA
# ============================================================

def transform_products(df_products):

    print("\n========== PRODUCT TRANSFORMATION ==========")

    # --------------------------------------------------------
    # A. Handle Missing Values
    # --------------------------------------------------------

    df_products = df_products.fillna({
        "brand": "unknown",
        "price": "0",
        "category": "unknown"
    })

    # Remove records where mandatory columns are null
    df_products = df_products.dropna(
        subset=["id", "title"]
    )

    # --------------------------------------------------------
    # B. Data Type Casting
    # --------------------------------------------------------

    df_products = (
        df_products

        .withColumn(
            "id",
            F.col("id").cast("int")
        )

        .withColumn(
            "price",
            F.col("price").cast("double")
        )
    )

    # --------------------------------------------------------
    # C. String Normalization
    # trim + lower + remove special characters
    # --------------------------------------------------------

    df_products = (
        df_products

        .withColumn(
            "title",
            F.trim(
                F.lower(
                    F.col("title")
                )
            )
        )

        .withColumn(
            "category",
            F.trim(
                F.lower(
                    F.col("category")
                )
            )
        )

        .withColumn(
            "brand",
            F.trim(
                F.lower(
                    F.col("brand")
                )
            )
        )
    )

    # Remove special characters from title

    df_products = df_products.withColumn(
        "title",
        F.regexp_replace(
            F.col("title"),
            r"[^a-zA-Z0-9\s]",
            ""
        )
    )

    # --------------------------------------------------------
    # D. Deduplication
    # --------------------------------------------------------

    df_products = df_products.dropDuplicates(
        ["id"]
    )

    # --------------------------------------------------------
    # E. Column Renaming
    # --------------------------------------------------------

    df_products = (
        df_products

        .withColumnRenamed(
            "id",
            "product_id"
        )

        .withColumnRenamed(
            "price",
            "base_price"
        )
    )

    # --------------------------------------------------------
    # F. Flatten Nested Dimensions JSON
    # --------------------------------------------------------

    dimension_schema = StructType([

        StructField(
            "width",
            DoubleType(),
            True
        ),

        StructField(
            "height",
            DoubleType(),
            True
        ),

        StructField(
            "depth",
            DoubleType(),
            True
        )
    ])

    df_products = df_products.withColumn(
        "dimensions",
        F.from_json(
            F.col("dimensions_json"),
            dimension_schema
        )
    )

    # Extract nested fields

    df_products = (
        df_products

        .withColumn(
            "product_width",
            F.col("dimensions.width")
        )

        .withColumn(
            "product_height",
            F.col("dimensions.height")
        )

        .withColumn(
            "product_depth",
            F.col("dimensions.depth")
        )
    )

    # --------------------------------------------------------
    # G. Business Logic
    # Price Classification
    # --------------------------------------------------------

    df_products = df_products.withColumn(
        "price_category",

        F.when(
            F.col("base_price") >= 500,
            "Premium"
        )

        .when(
            F.col("base_price") >= 100,
            "Mid Range"
        )

        .otherwise(
            "Budget"
        )
    )

    # --------------------------------------------------------
    # H. Select Required Columns
    # --------------------------------------------------------

    df_products = df_products.select(

        "product_id",
        "title",
        "category",
        "brand",
        "base_price",
        "price_category",
        "product_width",
        "product_height",
        "product_depth",
        "tags"
    )

    return df_products


# ============================================================
# 2. TRANSFORM CLICKSTREAM DATA
# ============================================================

def transform_clicks(df_clicks):

    print("\n========== CLICKSTREAM TRANSFORMATION ==========")

    # --------------------------------------------------------
    # A. Handle Missing Values
    # --------------------------------------------------------

    df_clicks = df_clicks.fillna({
        "device": "unknown",
        "action": "unknown",
        "email": "not_available"
    })

    # Remove records where mandatory fields are missing

    df_clicks = df_clicks.dropna(
        subset=[
            "session_id",
            "product_id"
        ]
    )

    # --------------------------------------------------------
    # B. Data Type Casting
    # --------------------------------------------------------

    df_clicks = (
        df_clicks

        .withColumn(
            "product_id",
            F.col("product_id").cast("int")
        )

        .withColumn(
            "event_timestamp",
            F.to_timestamp(
                F.col("event_timestamp")
            )
        )
    )

    # --------------------------------------------------------
    # C. String Normalization
    # --------------------------------------------------------

    df_clicks = (
        df_clicks

        .withColumn(
            "action",
            F.trim(
                F.lower(
                    F.col("action")
                )
            )
        )

        .withColumn(
            "device",
            F.trim(
                F.lower(
                    F.col("device")
                )
            )
        )
    )

    # --------------------------------------------------------
    # D. Deduplication
    # --------------------------------------------------------

    df_clicks = df_clicks.dropDuplicates(
        ["session_id"]
    )

    # --------------------------------------------------------
    # E. Business Logic
    # Conversion Flag
    # --------------------------------------------------------

    df_clicks = df_clicks.withColumn(
        "conversion_flag",

        F.when(
            F.col("action") == "add_to_cart",
            1
        )

        .otherwise(0)
    )

    # --------------------------------------------------------
    # F. Engagement Classification
    # --------------------------------------------------------

    df_clicks = df_clicks.withColumn(
        "engagement_type",

        F.when(
            F.col("action") == "add_to_cart",
            "High Intent"
        )

        .when(
            F.col("action") == "click",
            "Medium Intent"
        )

        .when(
            F.col("action") == "view",
            "Low Intent"
        )

        .otherwise(
            "Unknown"
        )
    )

    # --------------------------------------------------------
    # G. Date & Time Manipulation
    # --------------------------------------------------------

    df_clicks = (
        df_clicks

        .withColumn(
            "event_year",
            F.year(
                F.col("event_timestamp")
            )
        )

        .withColumn(
            "event_month",
            F.month(
                F.col("event_timestamp")
            )
        )

        .withColumn(
            "event_day",
            F.dayofmonth(
                F.col("event_timestamp")
            )
        )

        .withColumn(
            "event_hour",
            F.hour(
                F.col("event_timestamp")
            )
        )
    )

    # --------------------------------------------------------
    # H. Anonymization
    # Hash User ID
    # --------------------------------------------------------

    df_clicks = df_clicks.withColumn(
        "hashed_user_id",

        F.sha2(
            F.col("user_id"),
            256
        )
    )

    # --------------------------------------------------------
    # I. Mask Email
    # --------------------------------------------------------

    df_clicks = df_clicks.withColumn(
        "masked_email",

        F.regexp_replace(
            F.col("email"),
            r"(^.).*(@.*$)",
            "$1***$2"
        )
    )

    # --------------------------------------------------------
    # J. Drop Sensitive Information
    # --------------------------------------------------------

    df_clicks = df_clicks.drop(
        "user_id",
        "user_name",
        "email",
        "phone"
    )

    # --------------------------------------------------------
    # K. Parse Location JSON
    # --------------------------------------------------------

    location_schema = StructType([

        StructField(
            "country",
            StringType(),
            True
        ),

        StructField(
            "city",
            StringType(),
            True
        ),

        StructField(
            "latitude",
            DoubleType(),
            True
        ),

        StructField(
            "longitude",
            DoubleType(),
            True
        )
    ])

    df_clicks = df_clicks.withColumn(
        "location",

        F.from_json(
            F.col("location_json"),
            location_schema
        )
    )

    # --------------------------------------------------------
    # L. Flatten Location Struct
    # --------------------------------------------------------

    df_clicks = (
        df_clicks

        .withColumn(
            "country",
            F.lower(
                F.trim(
                    F.col("location.country")
                )
            )
        )

        .withColumn(
            "city",
            F.col("location.city")
        )

        .withColumn(
            "latitude",
            F.col("location.latitude")
        )

        .withColumn(
            "longitude",
            F.col("location.longitude")
        )
    )

    # --------------------------------------------------------
    # M. Drop Raw JSON
    # --------------------------------------------------------

    df_clicks = df_clicks.drop(
        "location_json",
        "location"
    )

    return df_clicks


# ============================================================
# 3. TRANSFORM SALES DATA
# ============================================================

def transform_sales(df_sales):

    print("\n========== SALES TRANSFORMATION ==========")

    # --------------------------------------------------------
    # A. Missing Values
    # --------------------------------------------------------

    df_sales = df_sales.fillna({
        "quantity_sold": "0",
        "sale_amount": "0",
        "store_location": "unknown"
    })

    # --------------------------------------------------------
    # B. Data Type Casting
    # --------------------------------------------------------

    df_sales = (
        df_sales

        .withColumn(
            "product_id",
            F.col("product_id")
            .cast("int")
        )

        .withColumn(
            "quantity_sold",
            F.col("quantity_sold")
            .cast("int")
        )

        .withColumn(
            "sale_amount",
            F.col("sale_amount")
            .cast("double")
        )

        .withColumn(
            "sale_date",
            F.to_date(
                F.col("sale_date")
            )
        )
    )

    # --------------------------------------------------------
    # C. String Normalization
    # --------------------------------------------------------

    df_sales = df_sales.withColumn(
        "store_location",

        F.trim(
            F.lower(
                F.col("store_location")
            )
        )
    )

    # --------------------------------------------------------
    # D. Deduplication
    # --------------------------------------------------------

    df_sales = df_sales.dropDuplicates(
        ["transaction_id"]
    )

    # --------------------------------------------------------
    # E. Remove Unnecessary Metadata
    # --------------------------------------------------------

    df_sales = df_sales.drop(
        "internal_comment"
    )

    # --------------------------------------------------------
    # F. Date Transformations
    # --------------------------------------------------------

    df_sales = (
        df_sales

        .withColumn(
            "sale_year",
            F.year(
                F.col("sale_date")
            )
        )

        .withColumn(
            "sale_month",
            F.month(
                F.col("sale_date")
            )
        )

        .withColumn(
            "sale_day",
            F.dayofmonth(
                F.col("sale_date")
            )
        )
    )

    # --------------------------------------------------------
    # G. datediff()
    # --------------------------------------------------------

    df_sales = df_sales.withColumn(
        "days_since_sale",

        F.datediff(
            F.current_date(),
            F.col("sale_date")
        )
    )

    # --------------------------------------------------------
    # H. date_add()
    # Expected Delivery Date
    # --------------------------------------------------------

    df_sales = df_sales.withColumn(
        "expected_delivery_date",

        F.date_add(
            F.col("sale_date"),
            7
        )
    )

    # --------------------------------------------------------
    # I. trunc()
    # First day of sale month
    # --------------------------------------------------------

    df_sales = df_sales.withColumn(
        "sale_month_start",

        F.trunc(
            F.col("sale_date"),
            "month"
        )
    )

    # --------------------------------------------------------
    # J. Business Logic
    # --------------------------------------------------------

    df_sales = df_sales.withColumn(
        "order_size",

        F.when(
            F.col("quantity_sold") >= 4,
            "Large"
        )

        .when(
            F.col("quantity_sold") >= 2,
            "Medium"
        )

        .otherwise(
            "Small"
        )
    )

    return df_sales


# ============================================================
# 4. JOIN SALES WITH PRODUCT MASTER
# ============================================================

def enrich_sales_with_products(
    df_sales,
    df_products
):

    print("\n========== SALES + PRODUCT JOIN ==========")

    df_sales_enriched = (

        df_sales.alias("s")

        .join(

            df_products.alias("p"),

            F.col("s.product_id")
            ==
            F.col("p.product_id"),

            "left"
        )

        .select(

            F.col("s.transaction_id"),

            F.col("s.customer_id"),

            F.col("s.product_id"),

            F.col("p.title")
                .alias("product_name"),

            F.col("p.category"),

            F.col("p.brand"),

            F.col("p.base_price"),

            F.col("p.price_category"),

            F.col("s.quantity_sold"),

            F.col("s.sale_amount"),

            F.col("s.sale_date"),

            F.col("s.store_location"),

            F.col("s.sale_year"),

            F.col("s.sale_month"),

            F.col("s.sale_day"),

            F.col("s.days_since_sale"),

            F.col("s.expected_delivery_date"),

            F.col("s.sale_month_start"),

            F.col("s.order_size")
        )
    )

    return df_sales_enriched


# ============================================================
# 5. JOIN CLICKSTREAM WITH PRODUCT MASTER
# ============================================================

def enrich_clicks_with_products(
    df_clicks,
    df_products
):

    print("\n========== CLICKSTREAM + PRODUCT JOIN ==========")

    df_clicks_enriched = (

        df_clicks.alias("c")

        .join(

            df_products.alias("p"),

            F.col("c.product_id")
            ==
            F.col("p.product_id"),

            "left"
        )

        .select(

            F.col("c.*"),

            F.col("p.title")
                .alias("product_name"),

            F.col("p.category"),

            F.col("p.brand"),

            F.col("p.base_price"),

            F.col("p.price_category")
        )
    )

    return df_clicks_enriched


# ============================================================
# 6. EXPLODE PRODUCT TAGS
# ============================================================

def transform_product_tags(df_products):

    print("\n========== EXPLODE PRODUCT TAGS ==========")

    df_product_tags = (

        df_products

        .select(
            "product_id",
            "title",
            "category",
            "tags"
        )

        .withColumn(
            "tag",
            F.explode(
                F.col("tags")
            )
        )

        .drop("tags")
    )

    return df_product_tags


# ============================================================
# 7. EXPLODE USER INTERESTS
# ============================================================

def transform_user_interests(df_clicks):

    print("\n========== EXPLODE USER INTERESTS ==========")

    df_user_interests = (

        df_clicks

        .select(
            "session_id",
            "product_id",
            "interests"
        )

        .withColumn(
            "interest",
            F.explode(
                F.col("interests")
            )
        )

        .drop("interests")
    )

    return df_user_interests


# ============================================================
# 8. SALES AGGREGATION
# ============================================================

def aggregate_sales(df_sales_enriched):

    print("\n========== SALES AGGREGATION ==========")

    df_product_summary = (

        df_sales_enriched

        .groupBy(

            "product_id",
            "product_name",
            "category"
        )

        .agg(

            F.sum(
                "quantity_sold"
            ).alias(
                "total_units_sold"
            ),

            F.sum(
                "sale_amount"
            ).alias(
                "total_revenue"
            ),

            F.count(
                "transaction_id"
            ).alias(
                "transaction_count"
            ),

            F.avg(
                "sale_amount"
            ).alias(
                "average_transaction"
            ),

            F.countDistinct(
                "customer_id"
            ).alias(
                "unique_customers"
            )
        )
    )

    return df_product_summary


# ============================================================
# 9. CATEGORY AGGREGATION
# ============================================================

def aggregate_category_sales(
    df_sales_enriched
):

    print("\n========== CATEGORY AGGREGATION ==========")

    df_category_summary = (

        df_sales_enriched

        .groupBy(
            "category"
        )

        .agg(

            F.sum(
                "quantity_sold"
            ).alias(
                "total_units_sold"
            ),

            F.sum(
                "sale_amount"
            ).alias(
                "total_revenue"
            ),

            F.avg(
                "sale_amount"
            ).alias(
                "average_sale"
            ),

            F.count(
                "transaction_id"
            ).alias(
                "total_transactions"
            ),

            F.countDistinct(
                "customer_id"
            ).alias(
                "unique_customers"
            )
        )
    )

    return df_category_summary


# ============================================================
# 10. MONTHLY AGGREGATION
# ============================================================

def aggregate_monthly_sales(
    df_sales_enriched
):

    print("\n========== MONTHLY AGGREGATION ==========")

    df_monthly_sales = (

        df_sales_enriched

        .groupBy(
            "sale_year",
            "sale_month"
        )

        .agg(

            F.sum(
                "sale_amount"
            ).alias(
                "monthly_revenue"
            ),

            F.sum(
                "quantity_sold"
            ).alias(
                "units_sold"
            ),

            F.countDistinct(
                "customer_id"
            ).alias(
                "unique_customers"
            )
        )
    )

    return df_monthly_sales


# ============================================================
# 11. WINDOW FUNCTIONS
# ============================================================

def apply_window_functions(
    df_product_summary
):

    print("\n========== WINDOW FUNCTIONS ==========")

    # --------------------------------------------------------
    # Window partitioned by category
    # --------------------------------------------------------

    category_window = (

        Window

        .partitionBy(
            "category"
        )

        .orderBy(
            F.col(
                "total_revenue"
            ).desc()
        )
    )

    # --------------------------------------------------------
    # RANK
    # --------------------------------------------------------

    df_product_summary = (
        df_product_summary

        .withColumn(
            "revenue_rank",

            F.rank()
            .over(category_window)
        )
    )

    # --------------------------------------------------------
    # DENSE RANK
    # --------------------------------------------------------

    df_product_summary = (
        df_product_summary

        .withColumn(
            "dense_revenue_rank",

            F.dense_rank()
            .over(category_window)
        )
    )

    # --------------------------------------------------------
    # ROW NUMBER
    # --------------------------------------------------------

    df_product_summary = (
        df_product_summary

        .withColumn(
            "row_number",

            F.row_number()
            .over(category_window)
        )
    )

    return df_product_summary


# ============================================================
# 12. RUNNING TOTAL
# ============================================================

def calculate_running_revenue(
    df_monthly_sales
):

    print("\n========== RUNNING TOTAL ==========")

    monthly_window = (

        Window

        .orderBy(
            "sale_year",
            "sale_month"
        )

        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow
        )
    )

    df_monthly_sales = (

        df_monthly_sales

        .withColumn(

            "running_revenue",

            F.sum(
                "monthly_revenue"
            ).over(
                monthly_window
            )
        )
    )

    return df_monthly_sales


# ============================================================
# 13. PERFORMANCE OPTIMIZATION
# ============================================================

def optimize_partitions(
    df_sales_enriched,
    df_category_summary
):

    print("\n========== PARTITION OPTIMIZATION ==========")

    # Repartition based on frequently used column

    df_sales_enriched = (

        df_sales_enriched

        .repartition(
            8,
            "category"
        )
    )

    # Reduce number of output partitions

    df_category_summary = (
        df_category_summary
        .coalesce(2)
    )

    return (
        df_sales_enriched,
        df_category_summary
    )


# ============================================================
# 14. MAIN TRANSFORM FUNCTION
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
    # Transform individual datasets
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
    # Explode
    # --------------------------------------------------------

    df_product_tags = (
        transform_product_tags(
            df_products
        )
    )

    df_user_interests = (
        transform_user_interests(
            df_clicks
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

    # --------------------------------------------------------
    # Window Functions
    # --------------------------------------------------------

    df_product_summary = (
        apply_window_functions(
            df_product_summary
        )
    )

    # --------------------------------------------------------
    # Running Total
    # --------------------------------------------------------

    df_monthly_sales = (
        calculate_running_revenue(
            df_monthly_sales
        )
    )

    # --------------------------------------------------------
    # Performance Optimization
    # --------------------------------------------------------

    (
        df_sales_enriched,
        df_category_summary
    ) = optimize_partitions(
        df_sales_enriched,
        df_category_summary
    )

    print("\n")
    print("=" * 60)
    print("TRANSFORMATION COMPLETED")
    print("=" * 60)

    # --------------------------------------------------------
    # Return all transformed datasets
    # --------------------------------------------------------

    return {

        "products":
            df_products,

        "clicks":
            df_clicks,

        "sales":
            df_sales,

        "sales_enriched":
            df_sales_enriched,

        "clicks_enriched":
            df_clicks_enriched,

        "product_tags":
            df_product_tags,

        "user_interests":
            df_user_interests,

        "product_summary":
            df_product_summary,

        "category_summary":
            df_category_summary,

        "monthly_sales":
            df_monthly_sales
    }
result = transform_data(
    df_products,
    df_clicks,
    df_sales
)

print("\n========== TRANSFORMED PRODUCTS ==========")
result["products"].show(10, truncate=False)

print("\n========== TRANSFORMED CLICKS ==========")
result["clicks"].show(10, truncate=False)

print("\n========== TRANSFORMED SALES ==========")
result["sales"].show(10, truncate=False)

print("\n========== SALES ENRICHED ==========")
result["sales_enriched"].show(10, truncate=False)

print("\n========== CLICKS ENRICHED ==========")
result["clicks_enriched"].show(10, truncate=False)

print("\n========== PRODUCT TAGS ==========")
result["product_tags"].show(10, truncate=False)

print("\n========== USER INTERESTS ==========")
result["user_interests"].show(10, truncate=False)

print("\n========== PRODUCT SUMMARY ==========")
result["product_summary"].show(10, truncate=False)

print("\n========== CATEGORY SUMMARY ==========")
result["category_summary"].show(10, truncate=False)

print("\n========== MONTHLY SALES ==========")
result["monthly_sales"].show(10, truncate=False)