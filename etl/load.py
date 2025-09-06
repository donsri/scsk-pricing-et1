import pandas as pd
import pyodbc
import os
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from etl.config import (
    AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_CONTAINER,
    AZURE_STORAGE_SAS, AZURE_STORAGE_KEY,
    AZURE_SQL_SERVER, AZURE_SQL_DATABASE, AZURE_SQL_USERNAME, 
    AZURE_SQL_PASSWORD, SQL_SCHEMA, ODBC_DRIVER
)
from etl.utils import utc_now_date_str

def _blob_service():
    """Create blob service client"""
    if AZURE_STORAGE_SAS:
        acc_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net{AZURE_STORAGE_SAS}"
        return BlobServiceClient(account_url=acc_url)
    else:
        acc_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
        return BlobServiceClient(account_url=acc_url, credential=AZURE_STORAGE_KEY)

def get_sql_connection():
    """Create SQL Server connection with Docker-friendly settings"""
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={AZURE_SQL_SERVER};"
        f"DATABASE={AZURE_SQL_DATABASE};"
        f"UID={AZURE_SQL_USERNAME};"
        f"PWD={AZURE_SQL_PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"  # Changed from 'no' to 'yes' for Docker
        "Connection Timeout=60;"        # Added longer timeout
        "Command Timeout=60;"           # Added command timeout
    )
    print(f"Connecting to: {AZURE_SQL_SERVER}")  # Debug info
    return pyodbc.connect(conn_str)

def read_sql_file(filename):
    """Read SQL file from sql directory"""
    sql_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sql')
    file_path = os.path.join(sql_dir, filename)
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file not found: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()

def execute_sql_script(sql_script, connection):
    """Execute SQL script with proper batch handling"""
    # Split script by GO statements and execute each batch
    batches = [batch.strip() for batch in sql_script.split('GO') if batch.strip()]
    
    if not batches:
        # If no GO statements, treat entire script as one batch
        batches = [sql_script]
    
    cursor = connection.cursor()
    
    for i, batch in enumerate(batches):
        if batch.strip():
            try:
                print(f"Executing SQL batch {i+1}/{len(batches)}")
                cursor.execute(batch)
                connection.commit()
            except Exception as e:
                print(f"Error in batch {i+1}: {e}")
                print(f"Batch content: {batch[:200]}...")
                raise
    
    cursor.close()

def create_products_table():
    """Create products table using SQL schema file"""
    print("Creating products table from SQL schema...")
    
    try:
        # Read the SQL schema file
        create_table_sql = read_sql_file('schema.sql')
        
        with get_sql_connection() as conn:
            execute_sql_script(create_table_sql, conn)
            print("Products table created/verified successfully")
            
    except Exception as e:
        print(f"Error creating table: {e}")
        raise

def test_sql_connection():
    """Test SQL Server connection"""
    try:
        print("Testing SQL Server connection...")
        with get_sql_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            print(f"Connection successful! SQL Server: {version[:50]}...")
            return True
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False

def find_latest_parquet():
    """Find latest processed parquet file for today"""
    blob_service = _blob_service()
    container = blob_service.get_container_client(AZURE_STORAGE_CONTAINER)
    
    today = utc_now_date_str()
    processed_prefix = f"processed/products/date={today}/"
    
    processed_blobs = list(container.list_blobs(name_starts_with=processed_prefix))
    
    if not processed_blobs:
        raise FileNotFoundError(f"No processed files found for today ({today}). Run transform first.")
    
    latest_file = sorted(processed_blobs, key=lambda x: x.name)[-1]
    print(f"Found processed file: {latest_file.name}")
    return latest_file.name

def load_parquet_from_blob(blob_path):
    """Load parquet file from Azure blob"""
    blob_service = _blob_service()
    container = blob_service.get_container_client(AZURE_STORAGE_CONTAINER)
    
    print(f"Loading parquet from: {blob_path}")
    parquet_data = container.download_blob(blob_path).readall()
    df = pd.read_parquet(BytesIO(parquet_data))
    print(f"Loaded {len(df)} records")
    return df

def load_to_sql(df):
    """Load DataFrame to SQL Server"""
    print(f"Loading {len(df)} records to SQL...")
    
    # Clear existing data
    with get_sql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {SQL_SCHEMA}.products")
        print("Cleared existing data")
        
        # Insert new data
        insert_sql = f"""
        INSERT INTO {SQL_SCHEMA}.products 
        (product_id, title, price_usd, price_gbp, description, category_name, 
        rating, rating_count, expensive, price_band, processing_date, 
        exchange_rate_used, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        for _, row in df.iterrows():
            cursor.execute(insert_sql, (
                int(row['product_id']),
                row['title'],
                float(row['price_usd']),
                float(row['price_gbp']),
                row['description'],
                row['category_name'],
                float(row['rating']) if pd.notna(row['rating']) else None,
                int(row['rating_count']) if pd.notna(row['rating_count']) else None,
                bool(row['expensive']),
                row['price_band'],
                pd.to_datetime(row['processing_date']).date(),
                float(row['exchange_rate_used']),
                pd.to_datetime(row['ingested_at'])
            ))
        
        conn.commit()
        print(f"Successfully loaded {len(df)} records")

def verify_load():
    """Verify data was loaded correctly"""
    with get_sql_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT COUNT(*) FROM {SQL_SCHEMA}.products")
        count = cursor.fetchone()[0]
        print(f"Total records in database: {count}")
        
        cursor.execute(f"SELECT TOP 3 product_id, title, price_gbp FROM {SQL_SCHEMA}.products")
        rows = cursor.fetchall()
        print("Sample records:")
        for row in rows:
            print(f"  ID: {row[0]}, Title: {row[1][:30]}..., Price: £{row[2]}")

def load_pipeline(parquet_path=None):
    """Main load pipeline"""
    print("Starting load process...")
    
    try:
        # Test connection first
        if not test_sql_connection():
            raise Exception("Cannot connect to SQL Server")
        
        # Find latest file if not specified
        if not parquet_path:
            parquet_path = find_latest_parquet()
        
        # Create table using SQL schema files
        create_products_table()
        
        # Load data
        df = load_parquet_from_blob(parquet_path)
        load_to_sql(df)
        verify_load()
        
        print("Load completed successfully!")
        return {"records_loaded": len(df), "table": "products"}
        
    except Exception as e:
        print(f"Load failed: {e}")
        raise

# if __name__ == "__main__":
#     print("Running load script...")
#     try:
#         result = load_pipeline()
#         print(f"SUCCESS: {result}")
#     except Exception as e:
#         print(f"FAILED: {e}")