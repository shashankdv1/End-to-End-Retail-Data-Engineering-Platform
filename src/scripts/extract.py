import pandas as pd
import requests
import sqlite3
import random
from faker import Faker

fake = Faker()


# ============================================================
# REST API SOURCE
# ============================================================

def fetch_api_products():

    print("\n========== REST API EXTRACTION START ==========")

    url = "https://dummyjson.com/products"

    response = requests.get(url)

    print("API Status Code:", response.status_code)

    if response.status_code == 200:

        products = response.json().get("products", [])

        print("Number of products received from API:", len(products))

        df_api = pd.DataFrame(products)[
            ["id", "title", "category", "price"]
        ]

        df_api = df_api.rename(
            columns={
                "id": "product_id",
                "price": "base_price"
            }
        )

        print("\nProduct DataFrame Type:")
        print(type(df_api))

        print("\nProduct Columns:")
        print(df_api.columns.tolist())

        print("\nProduct Shape:")
        print(df_api.shape)

        print("\nProduct Data:")
        print(df_api.head())

        print("\n========== REST API EXTRACTION COMPLETED ==========")

        return df_api

    raise Exception(
        f"API Connection Failed: Status {response.status_code}"
    )


# ============================================================
# FAKER SOURCE
# ============================================================

def generate_faker_interactions(num_records=500):

    print("\n========== FAKER EXTRACTION START ==========")

    print("Number of records requested:", num_records)

    interactions = []

    for _ in range(num_records):

        interactions.append({
            "session_id": fake.uuid4(),
            "user_id": fake.bothify(text='USR-####'),
            "product_id": random.randint(1, 50),
            "action": random.choice(
                ['view', 'add_to_cart', 'click']
            ),
            "device": random.choice(
                ['Mobile', 'Desktop', 'Tablet']
            )
        })

    df_clicks = pd.DataFrame(interactions)

    print("\nClick DataFrame Type:")
    print(type(df_clicks))

    print("\nClick Columns:")
    print(df_clicks.columns.tolist())

    print("\nClick Shape:")
    print(df_clicks.shape)

    print("\nClick Data:")
    print(df_clicks.head())

    print("\n========== FAKER EXTRACTION COMPLETED ==========")

    return df_clicks


# ============================================================
# SQLITE DATABASE SOURCE
# ============================================================

def fetch_database_sales():

    print("\n========== DATABASE EXTRACTION START ==========")

    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()

    cursor.execute(
        '''
        CREATE TABLE offline_sales(
            transaction_id TEXT,
            product_id INTEGER,
            quantity_sold INTEGER,
            date TEXT
        )
        '''
    )

    print("SQLite table created successfully.")

    mock_sales = [
        (
            fake.uuid4(),
            random.randint(1, 50),
            random.randint(1, 3),
            fake.date_this_month().strftime("%Y-%m-%d")
        )
        for _ in range(300)
    ]

    cursor.executemany(
        "INSERT INTO offline_sales VALUES (?, ?, ?, ?)",
        mock_sales
    )

    conn.commit()

    print("Number of records inserted:", len(mock_sales))

    df_db = pd.read_sql_query(
        "SELECT * FROM offline_sales",
        conn
    )

    print("\nSales DataFrame Type:")
    print(type(df_db))

    print("\nSales Columns:")
    print(df_db.columns.tolist())

    print("\nSales Shape:")
    print(df_db.shape)

    print("\nSales Data:")
    print(df_db.head())

    conn.close()

    print("\n========== DATABASE EXTRACTION COMPLETED ==========")

    return df_db


# ============================================================
# MASTER EXTRACT FUNCTION
# ============================================================

def extract_data():

    print("\n")
    print("=" * 60)
    print("STARTING EXTRACT LAYER")
    print("=" * 60)

    print("\nCalling Product API...")
    df_products = fetch_api_products()

    print("\nCalling Faker...")
    df_clicks = generate_faker_interactions()

    print("\nCalling SQLite...")
    df_sales = fetch_database_sales()

    print("\n")
    print("=" * 60)
    print("EXTRACT LAYER COMPLETED")
    print("=" * 60)

    print("\n========== FINAL EXTRACT SUMMARY ==========")

    print("\nProducts:")
    print("Type:", type(df_products))
    print("Shape:", df_products.shape)
    print(df_products.head(3))

    print("\nClicks:")
    print("Type:", type(df_clicks))
    print("Shape:", df_clicks.shape)
    print(df_clicks.head(3))

    print("\nSales:")
    print("Type:", type(df_sales))
    print("Shape:", df_sales.shape)
    print(df_sales.head(3))

    return df_products, df_clicks, df_sales


# ============================================================
# DEBUG RUN
# ============================================================

if __name__ == "__main__":

    df_products, df_clicks, df_sales = extract_data()

    print("\n========== EXTRACT OUTPUT ==========")

    print("\nPRODUCTS")
    print(df_products)

    print("\nCLICKS")
    print(df_clicks)

    print("\nSALES")
    print(df_sales)