# etl/config.py
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Azure Blob Storage settings
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_STORAGE_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "datalake")
AZURE_STORAGE_SAS = os.getenv("AZURE_STORAGE_SAS")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")

# Azure SQL settings
AZURE_SQL_SERVER = os.getenv("AZURE_SQL_SERVER")
AZURE_SQL_PORT = os.getenv("AZURE_SQL_PORT", "1433")
AZURE_SQL_DATABASE = os.getenv("AZURE_SQL_DATABASE")
AZURE_SQL_USERNAME = os.getenv("AZURE_SQL_USERNAME")
AZURE_SQL_PASSWORD = os.getenv("AZURE_SQL_PASSWORD")
SQL_SCHEMA = os.getenv("SQL_SCHEMA", "dbo")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

# Pipeline settings
PRICE_EXPENSIVE_THRESHOLD_GBP = float(os.getenv("PRICE_EXPENSIVE_THRESHOLD_GBP", "100"))

# ADD THESE LINES TO THE END OF YOUR EXISTING etl/config.py FILE:

def validate_config():
    """Validate that required environment variables are set"""
    print("Validating configuration...")
    
    config_status = {
        "AZURE_STORAGE_ACCOUNT_NAME": AZURE_STORAGE_ACCOUNT_NAME,
        "AZURE_STORAGE_CONTAINER": AZURE_STORAGE_CONTAINER,
        "AZURE_STORAGE_SAS": "SET" if AZURE_STORAGE_SAS else "NOT SET",
        "AZURE_STORAGE_KEY": "SET" if AZURE_STORAGE_KEY else "NOT SET",
        "AZURE_SQL_SERVER": AZURE_SQL_SERVER,
        "AZURE_SQL_DATABASE": AZURE_SQL_DATABASE,
        "AZURE_SQL_USERNAME": AZURE_SQL_USERNAME,
        "AZURE_SQL_PASSWORD": "SET" if AZURE_SQL_PASSWORD else "NOT SET",
    }
    
    print("\nConfiguration Status:")
    for key, value in config_status.items():
        status = "OK" if value and value != "NOT SET" else "MISSING"
        display_value = value[:20] + "..." if isinstance(value, str) and len(value) > 20 and value != "NOT SET" else value
        print(f"  [{status}] {key}: {display_value}")
    
    missing_vars = []
    if not AZURE_STORAGE_ACCOUNT_NAME:
        missing_vars.append("AZURE_STORAGE_ACCOUNT_NAME")
    if not AZURE_STORAGE_CONTAINER:
        missing_vars.append("AZURE_STORAGE_CONTAINER")
    if not AZURE_STORAGE_SAS and not AZURE_STORAGE_KEY:
        missing_vars.append("AZURE_STORAGE_SAS or AZURE_STORAGE_KEY")
    if not AZURE_SQL_SERVER:
        missing_vars.append("AZURE_SQL_SERVER")
    if not AZURE_SQL_DATABASE:
        missing_vars.append("AZURE_SQL_DATABASE")
    if not AZURE_SQL_USERNAME:
        missing_vars.append("AZURE_SQL_USERNAME")
    if not AZURE_SQL_PASSWORD:
        missing_vars.append("AZURE_SQL_PASSWORD")
    
    if missing_vars:
        print(f"\nMissing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    print("\nConfiguration validated successfully!")
    print(f"\nPipeline Settings:")
    print(f"   Expensive threshold: {PRICE_EXPENSIVE_THRESHOLD_GBP} GBP")
    
    return True

# if __name__ == "__main__":
#     print("Starting configuration validation...")
#     try:
#         validate_config()
#         print("\nConfiguration validation completed successfully!")
#     except Exception as e:
#         print(f"\nConfiguration validation failed: {e}")
#         exit(1)