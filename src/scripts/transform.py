from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def transform():

    spark = SparkSession.builder \
        .appName("Retail ETL") \
        .getOrCreate()

    products = spark.read.csv(
        "data/raw/products.csv",
        header=True,
        inferSchema=True
    )

    sales = spark.read.csv(
        "data/raw/sales.csv",
        header=True,
        inferSchema=True
    )

    clicks = spark.read.csv(
        "data/raw/click_logs.csv",
        header=True,
        inferSchema=True
    )

    final_df = sales.join(products, "product_id", "inner")

    final_df = final_df.withColumn(
        "revenue",
        col("quantity_sold") * col("base_price")
    )

    final_df.write.mode("overwrite") \
        .option("header", True) \
        .csv("data/processed/retail_sales")

    spark.stop()

    print("Transformation Completed")