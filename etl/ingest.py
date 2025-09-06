import json
import requests
from io import BytesIO
from azure.storage.blob import BlobServiceClient, ContentSettings
from etl.config import (
    AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_CONTAINER,
    AZURE_STORAGE_SAS, AZURE_STORAGE_KEY
)
from etl.utils import utc_now_date_str, utc_now_ts_str

FAKESTORE_URL = "https://fakestoreapi.com/products"
# Using a different free API that doesn't require authentication
FX_URL = "https://api.fxratesapi.com/latest?base=USD&currencies=GBP"

def _blob_service():
    print("Creating Azure Blob Service client...")
    print(f"   Account: {AZURE_STORAGE_ACCOUNT_NAME}")
    print(f"   Container: {AZURE_STORAGE_CONTAINER}")
    
    if AZURE_STORAGE_SAS:
        print("   Auth: Using SAS token")
        acc_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net{AZURE_STORAGE_SAS}"
        return BlobServiceClient(account_url=acc_url)
    else:
        print("   Auth: Using account key")
        acc_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
        return BlobServiceClient(account_url=acc_url, credential=AZURE_STORAGE_KEY)

def fetch_and_store_raw():
    """Fetch products + FX and store raw JSON in Azure Blob."""
    print("Starting data ingestion process...")
    
    try:
        date_str = utc_now_date_str()
        ts = utc_now_ts_str()
        print(f"Processing date: {date_str}")
        print(f"Timestamp: {ts}")

        print(f"\nFetching products from: {FAKESTORE_URL}")
        products_response = requests.get(FAKESTORE_URL, timeout=30)
        products_response.raise_for_status()
        products = products_response.json()
        print(f"Successfully fetched {len(products)} products")

        print(f"\nFetching exchange rates from: {FX_URL}")
        fx_response = requests.get(FX_URL, timeout=30)
        fx_response.raise_for_status()
        fx = fx_response.json()
        
        # Check if we got a valid response
        if "rates" in fx and "GBP" in fx["rates"]:
            gbp_rate = fx["rates"]["GBP"]
            print(f"Successfully fetched exchange rates")
            print(f"   USD to GBP rate: {gbp_rate}")
        else:
            # Fallback: create a mock exchange rate for testing
            print("Warning: Could not fetch live exchange rate, using fallback rate")
            gbp_rate = 0.79  # Approximate USD to GBP rate
            fx = {
                "base": "USD",
                "rates": {"GBP": gbp_rate},
                "date": date_str,
                "source": "fallback"
            }
            print(f"   USD to GBP rate (fallback): {gbp_rate}")

        print(f"\nConnecting to Azure Blob Storage...")
        blob_service = _blob_service()
        container = blob_service.get_container_client(AZURE_STORAGE_CONTAINER)
        
        print("Ensuring container exists...")
        try:
            container.create_container()
            print("Container created successfully")
        except Exception as e:
            if "ContainerAlreadyExists" in str(e) or "The specified container already exists" in str(e):
                print("Container already exists")
            else:
                print(f"Container creation issue: {e}")

        prod_path = f"raw/products/date={date_str}/products_{ts}.json"
        print(f"\nUploading products to: {prod_path}")
        prod_data = json.dumps(products, indent=2)
        container.upload_blob(
            name=prod_path,
            data=BytesIO(prod_data.encode("utf-8")),
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json")
        )
        print(f"Products uploaded successfully ({len(prod_data)} bytes)")

        fx_path = f"raw/fx/date={date_str}/fx_{ts}.json"
        print(f"\nUploading exchange rates to: {fx_path}")
        fx_data = json.dumps(fx, indent=2)
        container.upload_blob(
            name=fx_path,
            data=BytesIO(fx_data.encode("utf-8")),
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json")
        )
        print(f"Exchange rates uploaded successfully ({len(fx_data)} bytes)")

        result = {"products_blob": prod_path, "fx_blob": fx_path}
        
        print(f"\nIngestion completed successfully!")
        print(f"Files created:")
        print(f"   Products: {prod_path}")
        print(f"   Exchange rates: {fx_path}")
        
        return result
        
    except requests.RequestException as e:
        print(f"API request failed: {e}")
        raise
    except Exception as e:
        print(f"Ingestion failed: {e}")
        import traceback
        print("Full error details:")
        traceback.print_exc()
        raise

# if __name__ == "__main__":
#     print("Running ingestion script...")
#     try:
#         result = fetch_and_store_raw()
#         print(f"\nSUCCESS! Ingestion completed.")
#         print("Files written to Azure Blob Storage:")
#         for key, path in result.items():
#             print(f"   {key}: {path}")
#     except Exception as e:
#         print(f"\nFAILED! Error: {e}")
#         import traceback
#         traceback.print_exc()
#         exit(1)