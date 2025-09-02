from prefect import flow, task, get_run_logger
import pandas as pd
from io import BytesIO
import s3fs
import psycopg2
from psycopg2.extras import execute_values
from prefect.blocks.system import Secret, String
from prefect.blocks.core import Block
from typing import Dict, Any

# -------------------
# Configuration Management using Prefect Blocks
# -------------------

# You can manage these credentials securely in the Prefect UI
# For local testing, you can use environment variables or hardcode them
# but for production, use Secret blocks.

# Example of how you would load these securely
# s3_creds = Secret.load("s3-minio-creds")
# MINIO_ENDPOINT = s3_creds.get()['MINIO_ENDPOINT']
# ... etc.

# For this example, we'll keep the hardcoded values for simplicity
# but note that this is not a production best practice.
MINIO_ENDPOINT = "http://minio:9000"
MINIO_BUCKET = "sales"
ACCESS_KEY = "minio"
SECRET_KEY = "minio123"

PG_HOST = "analytics-DB"
PG_PORT = 5432
PG_USER = "admin"
PG_PASSWORD = "admin"
PG_DB = "sales_DB"

fs = s3fs.S3FileSystem(
    key=ACCESS_KEY,
    secret=SECRET_KEY,
    client_kwargs={"endpoint_url": MINIO_ENDPOINT}
)

# -------------------
# Prefect Tasks with Enhancements
# -------------------

@task
def extract_from_minio() -> pd.DataFrame:
    """
    Reads raw CSV from MinIO and returns a pandas DataFrame.

    :raises FileNotFoundError: if the specified file does not exist.
    :raises Exception: for other read errors.
    """
    logger = get_run_logger()
    path = f"{MINIO_BUCKET}/raw/sales.csv"
    try:
        logger.info(f"Attempting to extract data from {path}")
        with fs.open(path, "rb") as f:
            df = pd.read_csv(f, encoding='ISO-8859-1')
        logger.info(f"Successfully extracted {len(df)} rows from MinIO.")
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {path}")
        raise
    except Exception as e:
        logger.error(f"Failed to read data from MinIO: {e}")
        raise

@task
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and transforms the raw DataFrame.

    Handles null values, calculates 'SalesAmount', and casts columns to
    appropriate data types.
    """
    logger = get_run_logger()
    logger.info(f"Starting data cleaning process on {len(df)} rows.")

    initial_null_count = df.isnull().sum().sum()
    logger.info(f"Initial null values count: {initial_null_count}")

    df["CustomerID"] = df["CustomerID"].fillna(-1).astype(int)
    df["Description"] = df["Description"].fillna("unknown product").astype(str)
    df["SalesAmount"] = (df["Quantity"] * df["UnitPrice"]).astype(float)
    df["InvoiceNo"] = df["InvoiceNo"].astype(str)
    df["StockCode"] = df["StockCode"].astype(str)
    df["Quantity"] = df["Quantity"].astype(int)
    df["UnitPrice"] = df["UnitPrice"].astype(float)
    
    final_null_count = df.isnull().sum().sum()
    logger.info(f"Data cleaning complete. Final null values count: {final_null_count}")

    return df

@task
def build_star_schema(df: pd.DataFrame):
    """
    Constructs the star schema from the cleaned DataFrame.

    Creates and returns Fact and Dimension tables (DimDate, DimProduct, DimCustomer).
    """
    logger = get_run_logger()
    logger.info("Starting star schema build.")
    
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])

    dim_date = df[["InvoiceDate"]].drop_duplicates().reset_index(drop=True)
    dim_date["DateKey"] = dim_date["InvoiceDate"].dt.strftime("%Y%m%d").astype(int)
    dim_date["FullDate"] = dim_date["InvoiceDate"]
    dim_date["Day"] = dim_date["InvoiceDate"].dt.day
    dim_date["Month"] = dim_date["InvoiceDate"].dt.month
    dim_date["Quarter"] = dim_date["InvoiceDate"].dt.quarter
    dim_date["Year"] = dim_date["InvoiceDate"].dt.year
    dim_date["Weekday"] = dim_date["InvoiceDate"].dt.day_name()
    logger.info(f"Created DimDate table with {len(dim_date)} unique dates.")

    dim_product = df[["StockCode", "Description"]].drop_duplicates().reset_index(drop=True)
    dim_product["ProductKey"] = dim_product.index + 1
    logger.info(f"Created DimProduct table with {len(dim_product)} unique products.")

    dim_customer = df[["CustomerID", "Country"]].drop_duplicates().reset_index(drop=True)
    dim_customer["CustomerKey"] = dim_customer.index + 1
    logger.info(f"Created DimCustomer table with {len(dim_customer)} unique customers.")

    fact = df.merge(dim_date, on="InvoiceDate", how="left")
    fact = fact.merge(dim_product, on=["StockCode", "Description"], how="left")
    fact = fact.merge(dim_customer, on=["CustomerID", "Country"], how="left")
    
    fact_sales = fact[[
        "InvoiceNo", "DateKey", "ProductKey", "CustomerKey",
        "Quantity", "UnitPrice", "SalesAmount"
    ]]
    logger.info(f"Created FactSales table with {len(fact_sales)} rows.")

    return fact_sales, dim_date, dim_product, dim_customer

@task
def save_to_minio(fact: pd.DataFrame, dim_date: pd.DataFrame, dim_product: pd.DataFrame, dim_customer: pd.DataFrame):
    """
    Saves star schema tables to MinIO in CSV format.
    """
    logger = get_run_logger()
    tables = {
        "FactSales": fact,
        "DimDate": dim_date,
        "DimProduct": dim_product,
        "DimCustomer": dim_customer
    }
    
    for name, df in tables.items():
        try:
            path = f"{MINIO_BUCKET}/cleaned_data/{name}.csv"
            logger.info(f"Saving {name} table to {path}")
            with fs.open(path, "wb") as f:
                df.to_csv(f, index=False, encoding='utf-8')
            logger.info(f"Successfully saved {name}.")
        except Exception as e:
            logger.error(f"Failed to save {name} to MinIO: {e}")
            raise

@task
def create_tables_postgres():
    """
    Creates the necessary star schema tables in PostgreSQL if they do not exist.
    """
    logger = get_run_logger()
    conn = None
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DB
        )
        cur = conn.cursor()
        logger.info("Connected to PostgreSQL for table creation.")

        # SQL statements to create tables
        create_table_sql = [
            """
            CREATE TABLE IF NOT EXISTS DimDate (
                DateKey INT PRIMARY KEY,
                FullDate DATE NOT NULL,
                Day INT,
                Month INT,
                Quarter INT,
                Year INT,
                Weekday VARCHAR(20)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS DimProduct (
                ProductKey INT PRIMARY KEY,
                StockCode VARCHAR(50) NOT NULL,
                Description TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS DimCustomer (
                CustomerKey INT PRIMARY KEY,
                CustomerID INT NOT NULL,
                Country VARCHAR(50)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS FactSales (
                InvoiceNo VARCHAR(50) NOT NULL,
                DateKey INT NOT NULL REFERENCES DimDate(DateKey),
                ProductKey INT NOT NULL REFERENCES DimProduct(ProductKey),
                CustomerKey INT NOT NULL REFERENCES DimCustomer(CustomerKey),
                Quantity INT,
                UnitPrice NUMERIC,
                SalesAmount NUMERIC
            )
            """
        ]

        for sql in create_table_sql:
            cur.execute(sql)
            logger.info("Executed table creation statement.")

        conn.commit()
        logger.info("All tables created successfully.")

    except psycopg2.Error as e:
        logger.error(f"PostgreSQL error during table creation: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.info("PostgreSQL connection closed after table creation.")


@task
def load_to_postgres(fact: pd.DataFrame, dim_date: pd.DataFrame, dim_product: pd.DataFrame, dim_customer: pd.DataFrame):
    """
    Loads star schema tables into PostgreSQL.
    """
    logger = get_run_logger()
    conn = None
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DB
        )
        cur = conn.cursor()
        logger.info("Successfully connected to PostgreSQL.")

        table_order = {
            "DimDate": dim_date,
            "DimProduct": dim_product,
            "DimCustomer": dim_customer,
            "FactSales": fact
        }
        
        # We need to load dimensions first due to foreign key constraints
        for name, df in table_order.items():
            logger.info(f"Loading data into {name} table...")
            
            # Using execute_values for efficient bulk insert
            if name == "DimDate":
                values = list(df[["DateKey","FullDate","Day","Month","Quarter","Year","Weekday"]].itertuples(index=False, name=None))
                query = "INSERT INTO DimDate (DateKey, FullDate, Day, Month, Quarter, Year, Weekday) VALUES %s ON CONFLICT (DateKey) DO NOTHING"
            elif name == "DimProduct":
                values = list(df[["ProductKey","StockCode","Description"]].itertuples(index=False, name=None))
                query = "INSERT INTO DimProduct (ProductKey, StockCode, Description) VALUES %s ON CONFLICT (ProductKey) DO NOTHING"
            elif name == "DimCustomer":
                values = list(df[["CustomerKey","CustomerID","Country"]].itertuples(index=False, name=None))
                query = "INSERT INTO DimCustomer (CustomerKey, CustomerID, Country) VALUES %s ON CONFLICT (CustomerKey) DO NOTHING"
            elif name == "FactSales":
                values = list(df.itertuples(index=False, name=None))
                query = "INSERT INTO FactSales (InvoiceNo, DateKey, ProductKey, CustomerKey, Quantity, UnitPrice, SalesAmount) VALUES %s"
            
            execute_values(cur, query, values)
            logger.info(f"Successfully loaded {len(df)} rows into {name}.")

        conn.commit()
        logger.info("All data successfully committed to database.")
    
    except psycopg2.Error as e:
        logger.error(f"PostgreSQL database error: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during PostgreSQL load: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.info("Database connection closed.")


# -------------------
# Prefect Flow with Enhancements
# -------------------

@flow(
    name="Sales ETL to Star Schema", 
    # Add retries for resilience
    retries=3,
    retry_delay_seconds=60,
    # Add a custom description for the flow run in the UI
    description="An ETL pipeline that extracts sales data, transforms it into a star schema, and loads it into a PostgreSQL data warehouse."
)
def sales_etl_flow():
    """Main ETL flow for sales data."""
    logger = get_run_logger()
    logger.info("Starting the Sales ETL Flow.")
    
    try:
        
        raw_df = extract_from_minio()
        cleaned_df = clean_data(raw_df)
        fact, dim_date, dim_product, dim_customer = build_star_schema(cleaned_df)
        
        # Parallelize the saving and loading tasks
        save = save_to_minio(fact, dim_date, dim_product, dim_customer)
        create = create_tables_postgres()
        load = load_to_postgres(fact, dim_date, dim_product, dim_customer)
        
    except Exception as e:
        logger.error(f"Flow failed with a critical error: {e}")
        # You could add notification logic here, e.g., send an email or Slack message
        # from prefect.blocks.notifications import SlackWebhook
        # slack_webhook_block = SlackWebhook.load("slack-notifier")
        # slack_webhook_block.notify(f"Flow 'Sales ETL to Star Schema' failed: {e}")
        raise
