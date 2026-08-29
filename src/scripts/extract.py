import pandas as pd
import requests
import sqlite3
import random
from faker import Faker
fake=Faker()

#REST API SOURCE

def fetch_api_products():
    print("Extracting data from REST API...")
    url = "https://dummyjson.com/products"
    response = requests.get(url)
    if response.status_code == 200:
        products = response.json().get("products",[])
        df_api = pd.DataFrame(products)[["id","title","category","price"]]
        df_api = df_api.rename(
            columns={"id":"product_id","price":"base_price"}
        )
        return df_api
    raise Exception(f"API Connection Failed: Status {response.status_code}")

#Python Faker SOURCE(Real-time User Interaction/click logs)

def generate_faker_interactions(num_records=500):
    print("Generating User Click logs via Faker...")
    interactions=[]
    for _ in range(num_records):
        interactions.append({
            "session_id": fake.uuid4(),
            "user_id": fake.bothify(text='USR-####'),
            "product_id": random.randint(1,50),
            "action": random.choice(['view','add_to_cart','click']),
            "device": random.choice(['Mobile','Desktop','Tablet'])
        }) 
    return pd.DataFrame(interactions)

#Relational DB Source

def fetch_database_Sales():
    print("Extracting historical transactions from SQLite3 Database")
    #Initialize a temporary in-memory database for file testing
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()

    #create a mock historical transactions table
    cursor.execute(
        '''CREATE TABLE offline_sales(
        transaction_id TEXT,
        product_id INTEGER,
        quantity_sold INTEGER,
        date TEXT
        )''')

    #Insert some seed records
     # Insert some seed records
    mock_sales = [
        (fake.uuid4(), random.randint(1, 50), random.randint(1, 3), fake.date_this_month().strftime("%Y-%m-%d"))
        for _ in range(300)
    ]
    cursor.executemany("INSERT INTO offline_sales VALUES (?, ?, ?, ?)", mock_sales)
    conn.commit()
    
    # Extract data out using Pandas SQL reading capabilities
    df_db = pd.read_sql_query("SELECT * FROM offline_sales", conn)
    conn.close()
    return df_db

# Load extract processes into extract_data Function
def extract_data():
    df_products = fetch_api_products()
    df_clicks = generate_faker_interactions()
    df_sales = fetch_database_Sales()
    return df_products,df_clicks,df_sales
