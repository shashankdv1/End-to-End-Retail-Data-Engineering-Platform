# ============================================================
# load.py
# POSTGRESQL LOAD LAYER
# ============================================================

import psycopg2
from sqlalchemy import create_engine
from extract import extract_data
from transform import transform_data


# ============================================================
# POSTGRESQL CONFIGURATION
# ============================================================

DB_HOST = "postgres"
DB_PORT = ""
DB_NAME = "sales_db"
DB_USER = ""
DB_PASSWORD = ""


# ============================================================
# CREATE POSTGRESQL CONNECTION
# ============================================================

def create_postgres_engine():

    print("\n========== CREATING POSTGRES CONNECTION ==========")

    connection_string = (
        f"postgresql+psycopg2://"
        f"{DB_USER}:{DB_PASSWORD}@"
        f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    engine = create_engine(
        connection_string
    )

    print("PostgreSQL connection created successfully.")

    return engine


# ============================================================
# TEST DATABASE CONNECTION
# ============================================================

def test_connection(engine):

    print("\n========== TESTING POSTGRES CONNECTION ==========")

    try:

        with engine.connect() as connection:

            print(
                "PostgreSQL connection successful."
            )

    except Exception as e:

        print(
            "PostgreSQL connection failed."
        )

        print("Error:", e)

        raise


# ============================================================
# LOAD SINGLE DATAFRAME
# ============================================================

def load_dataframe(
    df,
    table_name,
    engine,
    if_exists="replace"
):

    print(
        f"\n========== LOADING {table_name.upper()} =========="
    )

    print(
        "Rows:",
        len(df)
    )

    print(
        "Columns:",
        df.columns.tolist()
    )

    try:

        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=1000
        )

        print(
            f"Successfully loaded {table_name}"
        )

    except Exception as e:

        print(
            f"Failed to load {table_name}"
        )

        print("Error:", e)

        raise


# ============================================================
# LOAD ALL TRANSFORMED DATA
# ============================================================

def load_data(transformed_data):

    print("\n")
    print("=" * 60)
    print("STARTING LOAD LAYER")
    print("=" * 60)

    # --------------------------------------------------------
    # Create PostgreSQL Engine
    # --------------------------------------------------------

    engine = create_postgres_engine()

    # --------------------------------------------------------
    # Test Connection
    # --------------------------------------------------------

    test_connection(engine)

    # --------------------------------------------------------
    # Get transformed DataFrames
    # --------------------------------------------------------

    df_products = transformed_data[
        "products"
    ]

    df_clicks = transformed_data[
        "clicks"
    ]

    df_sales = transformed_data[
        "sales"
    ]

    df_sales_enriched = transformed_data[
        "sales_enriched"
    ]

    df_clicks_enriched = transformed_data[
        "clicks_enriched"
    ]

    df_product_summary = transformed_data[
        "product_summary"
    ]

    df_category_summary = transformed_data[
        "category_summary"
    ]

    df_monthly_sales = transformed_data[
        "monthly_sales"
    ]

    # --------------------------------------------------------
    # Load Product Master
    # --------------------------------------------------------

    load_dataframe(
        df_products,
        "dim_products",
        engine
    )

    # --------------------------------------------------------
    # Load Clickstream
    # --------------------------------------------------------

    load_dataframe(
        df_clicks,
        "fact_clicks",
        engine
    )

    # --------------------------------------------------------
    # Load Sales
    # --------------------------------------------------------

    load_dataframe(
        df_sales,
        "fact_sales",
        engine
    )

    # --------------------------------------------------------
    # Load Enriched Sales
    # --------------------------------------------------------

    load_dataframe(
        df_sales_enriched,
        "fact_sales_enriched",
        engine
    )

    # --------------------------------------------------------
    # Load Enriched Clickstream
    # --------------------------------------------------------

    load_dataframe(
        df_clicks_enriched,
        "fact_clicks_enriched",
        engine
    )

    # --------------------------------------------------------
    # Load Product Summary
    # --------------------------------------------------------

    load_dataframe(
        df_product_summary,
        "product_sales_summary",
        engine
    )

    # --------------------------------------------------------
    # Load Category Summary
    # --------------------------------------------------------

    load_dataframe(
        df_category_summary,
        "category_sales_summary",
        engine
    )

    # --------------------------------------------------------
    # Load Monthly Summary
    # --------------------------------------------------------

    load_dataframe(
        df_monthly_sales,
        "monthly_sales_summary",
        engine
    )

    # --------------------------------------------------------
    # Close Connection
    # --------------------------------------------------------

    engine.dispose()

    print("\n")
    print("=" * 60)
    print("LOAD COMPLETED SUCCESSFULLY")
    print("=" * 60)


# ============================================================
# MAIN ETL PIPELINE
# ============================================================

if __name__ == "__main__":

    try:

        print("\n")
        print("=" * 60)
        print("STARTING ETL PIPELINE")
        print("=" * 60)

        # ----------------------------------------------------
        # EXTRACT
        # ----------------------------------------------------

        print("\n========== EXTRACT ==========")

        df_products, df_clicks, df_sales = (
            extract_data()
        )

        # ----------------------------------------------------
        # TRANSFORM
        # ----------------------------------------------------

        print("\n========== TRANSFORM ==========")

        transformed_data = transform_data(
            df_products,
            df_clicks,
            df_sales
        )

        # ----------------------------------------------------
        # LOAD
        # ----------------------------------------------------

        load_data(
            transformed_data
        )

        print("\n")
        print("=" * 60)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)

    except Exception as e:

        print("\n")
        print("=" * 60)
        print("ETL PIPELINE FAILED")
        print("=" * 60)

        print("Error:", e)

        raise
