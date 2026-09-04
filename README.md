# Sales Data Pipeline

An ETL data pipeline that extracts product, user-interaction, and sales data from multiple sources, transforms the data, and loads the results into PostgreSQL. The project is designed to run with Docker/Airflow and uses Python, Pandas, PySpark, SQLAlchemy, and PostgreSQL connectivity through Psycopg2.

## Architecture

```text
                         EXTRACT
                            |
          +-----------------+-----------------+
          |                 |                 |
      REST API          Faker Logs        SQLite DB
          |                 |                 |
          v                 v                 v
      Products           Clicks             Sales
          +-----------------+-----------------+
                            |
                            v
                        TRANSFORM
                            |
                     PySpark DataFrames
                            |
          +-----------------+-----------------+
          |                 |                 |
       Cleaning          Joins          Aggregations
       Casting           Enrichment     Window Functions
       Deduplication     Explode         Running Totals
                            |
                            v
                           LOAD
                            |
                       PostgreSQL
                            |
                         Airflow
```

## Data Sources

### 1. REST API

Product data is extracted from the DummyJSON products API.

Source URL:

```text
https://dummyjson.com/products
```

The extract layer selects product attributes including:

- `id`
- `title`
- `category`
- `price`

The extracted product columns are renamed to:

- `product_id`
- `base_price`

### 2. Faker

Synthetic user click/interactions are generated using Faker and Python's `random` module.

The current extract implementation generates fields such as:

- `session_id`
- `user_id`
- `product_id`
- `action`
- `device`

The default number of generated interaction records is 500.

### 3. SQLite

A temporary in-memory SQLite database is created for test data. Historical sales records are inserted into an `offline_sales` table and read using Pandas.

The current sales source contains:

- `transaction_id`
- `product_id`
- `quantity_sold`
- `date`

The default number of generated sales records is 300.

## Tech Stack

| Technology | Purpose |
|---|---|
| Python | Pipeline implementation |
| Pandas | Extract-layer data handling |
| Requests | REST API calls |
| Faker | Synthetic clickstream generation |
| SQLite3 | Mock relational source |
| PySpark | Transform layer and distributed DataFrame processing |
| SQLAlchemy | PostgreSQL database engine/connection |
| Psycopg2-binary | PostgreSQL Python driver |
| PostgreSQL | Target database |
| Apache Airflow | Pipeline orchestration |
| Docker | Containerized execution |

## Python Dependencies

`requirements.txt` should contain one package per line:

```text
pandas
requests
faker
psycopg2-binary
sqlalchemy
pyspark
```

These are Python package dependencies installed with `pip`.

## Project Structure

A typical layout for the project is:

```text
Real_Time_Sales_Data_Pipeline/
|
+-- src/
|   +-- scripts/
|       +-- extract.py
|       +-- transform.py
|       +-- load.py
|       +-- transform_notebook.ipynb
|
+-- requirements.txt
+-- Dockerfile
+-- docker-compose.yml
+-- README.md
```

Adjust the structure if your local repository contains additional folders or DAG files.

## Extract Layer

The Extract layer contains an `extract_data()` function that calls the three source-specific extraction functions and returns:

```python
return df_products, df_clicks, df_sales
```

At this stage the data is handled as Pandas DataFrames.

## Transform Layer

The Transform layer is intended to operate on PySpark DataFrames.

The transformation workflow includes:

### Product transformations

- Missing-value handling
- Mandatory-field validation
- Data type casting
- String trimming and lowercasing
- Special-character removal
- Deduplication
- Column renaming
- Price classification

Price classification rules:

```text
base_price >= 500  -> Premium
base_price >= 100  -> Mid Range
otherwise           -> Budget
```

### Clickstream transformations

- Missing-value handling
- Product ID casting
- Timestamp conversion
- String normalization
- Session deduplication
- Conversion flag creation
- Engagement classification
- Date/time extraction
- User ID hashing
- Email masking
- Sensitive-column removal
- Location JSON parsing and flattening

Engagement classification:

```text
add_to_cart -> High Intent
click       -> Medium Intent
view        -> Low Intent
otherwise   -> Unknown
```

### Sales transformations

- Missing-value handling
- Product ID and quantity casting
- Sales amount casting
- Date conversion
- Store-location normalization
- Transaction deduplication
- Date-part extraction
- `datediff()` for elapsed days
- `date_add()` for expected delivery date
- `trunc()` for month start
- Order-size classification

Order-size classification:

```text
quantity_sold >= 4 -> Large
quantity_sold >= 2 -> Medium
otherwise           -> Small
```

### Enrichment

The transform layer enriches sales and clickstream data by joining them with the product master on `product_id` using a left join.

### Explode transformations

The transform design includes exploding array-valued product tags and user interests into individual rows.

### Aggregations

The pipeline includes:

- Product-level sales aggregation
- Category-level sales aggregation
- Monthly sales aggregation
- Total units sold
- Revenue calculations
- Transaction counts
- Average transaction/sale calculations
- Distinct customer counts

### Window functions

The product summary uses a category-partitioned window ordered by revenue descending and calculates:

- `rank()`
- `dense_rank()`
- `row_number()`

A monthly running revenue total is also calculated using an ordered window from the first row through the current row.

### Partition optimization

The transform layer also includes repartitioning sales-enriched data by category and coalescing category-summary output partitions.

## Load Layer

The Load layer uses SQLAlchemy with Psycopg2 to connect to PostgreSQL and write transformed data.

The SQLAlchemy connection is constructed from:

```text
DB_USER
DB_PASSWORD
DB_HOST
DB_PORT
DB_NAME
```

For the current Docker PostgreSQL setup, the connection settings should use:

```text
DB_HOST=postgres
DB_PORT=5432
DB_USER=airflow
DB_NAME=retail_db
```

Do not use `sales_db` unless that database is explicitly created.

## Docker Setup

The current Docker environment includes containers named:

```text
airflow
postgres
```

The PostgreSQL container uses the PostgreSQL image and is available to the Airflow container through the Docker service hostname:

```text
postgres
```

### Check running containers

```bash
docker ps
```

Expected services include:

```text
NAMES      IMAGE
airflow    dockerfile-airflow
postgres   postgres:16
```

### Check PostgreSQL environment

```bash
docker exec postgres env | findstr POSTGRES
```

The current setup uses:

```text
POSTGRES_DB=retail_db
POSTGRES_USER=airflow
```

### Connect to PostgreSQL

```bash
docker exec -it postgres psql -U airflow -d retail_db
```

Check available databases:

```sql
\l
```

Check the current database and user:

```sql
SELECT current_database();
SELECT current_user;
```

## Building the Docker Image

Build the Airflow image with Docker Compose:

```bash
docker compose build --no-cache airflow
```

Or rebuild and start the services:

```bash
docker compose up --build -d
```

### Requirements installation

The Dockerfile installs dependencies using:

```dockerfile
RUN pip install --no-cache-dir -r requirements.txt
```

Make sure every requirement is on a separate line. For example, this is correct:

```text
psycopg2-binary
sqlalchemy
```

and this is incorrect:

```text
psycopg2-binary sqlalchemy
```

## Debugging

### Extract debugging

The Extract layer uses Pandas, so inspect data with:

```python
print(type(df_products))
print(df_products.shape)
print(df_products.columns.tolist())
print(df_products.head())
```

### Transform debugging

Because the Transform layer uses PySpark, use:

```python
print(type(df_products))
df_products.show(5, truncate=False)
df_products.printSchema()
```

For the final transformed result:

```python
result = transform_data(
    df_products,
    df_clicks,
    df_sales
)

result["products"].show(10, truncate=False)
result["clicks"].show(10, truncate=False)
result["sales"].show(10, truncate=False)
```

### Notebook debugging

Use `transform_notebook.ipynb` as a Jupyter notebook. Do not execute the notebook file directly with:

```bash
python transform_notebook.ipynb
```

A notebook is JSON, so Python can raise errors such as:

```text
NameError: name 'null' is not defined
```

Open the notebook in VS Code/Jupyter and run its cells normally.

## Common Issues and Fixes

### `AttributeError: 'DataFrame' object has no attribute 'withColumn'`

Cause: a Pandas DataFrame was passed to PySpark transformation code.

The transform layer uses PySpark methods such as:

```python
df.withColumn(...)
df.dropDuplicates(...)
df.groupBy(...)
df.join(...)
```

Make sure the object entering the Transform layer is a PySpark DataFrame.

### `KeyError: ['id']`

Cause: the Extract layer already renamed `id` to `product_id`.

Use the same schema consistently between Extract and Transform.

### PostgreSQL `database "sales_db" does not exist`

Cause: the Load layer points to a database that does not exist in the PostgreSQL container.

For the current setup, use:

```text
DB_NAME=retail_db
```

### PostgreSQL `role "postgres" does not exist`

Cause: the container was initialized with:

```text
POSTGRES_USER=airflow
```

Use `airflow` as the database user.

### Docker build error: `Invalid requirement`

Cause: multiple package names were placed on one `requirements.txt` line.

Use one package per line.

## End-to-End Flow

The intended execution sequence is:

```text
1. Start PostgreSQL and Airflow containers
2. Extract data from REST API, Faker, and SQLite
3. Pass extracted data into the Transform layer
4. Perform PySpark cleaning and enrichment
5. Generate aggregated/transformed DataFrames
6. Load the transformed outputs into PostgreSQL
7. Use Airflow for orchestration and monitoring
```

## Database Connection Example

A SQLAlchemy engine function can be structured as:

```python
def create_postgres_engine():

    print("\n========== CREATING POSTGRES CONNECTION ==========")

    connection_string = (
        f"postgresql+psycopg2://"
        f"{DB_USER}:{DB_PASSWORD}@"
        f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    engine = create_engine(connection_string)

    print("PostgreSQL connection created successfully.")

    return engine
```

The values must match the running PostgreSQL container configuration.

## Notes

- The REST API, Faker, and SQLite sources are suitable for project/demo testing.
- Faker and SQLite data are generated dynamically, so row values can change between runs.
- Keep database credentials in environment variables or a secret-management mechanism rather than hard-coding passwords in source control.
- Keep the Extract schema and Transform expectations synchronized when adding or renaming fields.
