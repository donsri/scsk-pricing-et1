import json
import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from etl.config import (
    AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_CONTAINER,
    AZURE_STORAGE_SAS, AZURE_STORAGE_KEY, PRICE_EXPENSIVE_THRESHOLD_GBP
)
from etl.utils import utc_now_date_str, utc_now_ts_str

def _blob_service():
    """Create blob service client"""
    if AZURE_STORAGE_SAS:
        acc_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net{AZURE_STORAGE_SAS}"
        return BlobServiceClient(account_url=acc_url)
    else:
        acc_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
        return BlobServiceClient(account_url=acc_url, credential=AZURE_STORAGE_KEY)

def _find_latest_files():
    """Find the most recent ingestion files for today"""
    print("Looking for today's ingestion files...")
    
    blob_service = _blob_service()
    container = blob_service.get_container_client(AZURE_STORAGE_CONTAINER)
    
    today = utc_now_date_str()
    
    products_prefix = f"raw/products/date={today}/"
    products_blobs = list(container.list_blobs(name_starts_with=products_prefix))
    
    fx_prefix = f"raw/fx/date={today}/"
    fx_blobs = list(container.list_blobs(name_starts_with=fx_prefix))
    
    if not products_blobs:
        raise FileNotFoundError(f"No product files found for today ({today}). Run ingestion first.")
    
    if not fx_blobs:
        raise FileNotFoundError(f"No FX files found for today ({today}). Run ingestion first.")
    
    latest_products = sorted(products_blobs, key=lambda x: x.name)[-1]
    latest_fx = sorted(fx_blobs, key=lambda x: x.name)[-1]
    
    print(f"Found products file: {latest_products.name}")
    print(f"Found FX file: {latest_fx.name}")
    
    return latest_products.name, latest_fx.name

def _download_json_blob(container, path):
    """Download and parse JSON blob"""
    try:
        blob = container.get_blob_client(path)
        data = blob.download_blob().readall().decode("utf-8")
        return json.loads(data)
    except Exception as e:
        print(f"Error downloading blob {path}: {e}")
        raise

def _extract_gbp_rate(fx_data):
    """Extract GBP rate from different FX API response formats"""
    print("FX data structure:", fx_data)
    
    # Try different possible structures
    if isinstance(fx_data, dict):
        # Standard exchangerate.host format
        if "rates" in fx_data and "GBP" in fx_data["rates"]:
            return fx_data["rates"]["GBP"]
        
        # Direct format
        if "GBP" in fx_data:
            return fx_data["GBP"]
        
        # Check if there's a conversion_rates key
        if "conversion_rates" in fx_data and "GBP" in fx_data["conversion_rates"]:
            return fx_data["conversion_rates"]["GBP"]
        
        # Check for other possible rate keys
        for key in ["rates", "data", "result"]:
            if key in fx_data and isinstance(fx_data[key], dict) and "GBP" in fx_data[key]:
                return fx_data[key]["GBP"]
    
    # If we can't find the rate, show available keys
    if isinstance(fx_data, dict):
        print(f"Available keys in FX data: {list(fx_data.keys())}")
        for key, value in fx_data.items():
            if isinstance(value, dict):
                print(f"Keys in '{key}': {list(value.keys())}")
    
    raise ValueError(f"Could not find GBP exchange rate in FX data. Structure: {fx_data}")

def transform_to_parquet(products_blob_path=None, fx_blob_path=None):
    """Transform raw JSON data to processed Parquet format"""
    print("Starting transformation process...")
    
    try:
        if not products_blob_path or not fx_blob_path:
            print("Auto-discovering latest ingestion files...")
            products_blob_path, fx_blob_path = _find_latest_files()
        
        print(f"Processing:")
        print(f"  Products: {products_blob_path}")
        print(f"  FX: {fx_blob_path}")
        
        blob_service = _blob_service()
        container = blob_service.get_container_client(AZURE_STORAGE_CONTAINER)

        print("Downloading products data...")
        products = _download_json_blob(container, products_blob_path)
        print(f"Downloaded {len(products)} products")

        print("Downloading exchange rates...")
        fx = _download_json_blob(container, fx_blob_path)
        
        # Use robust rate extraction
        rate_usd_gbp = _extract_gbp_rate(fx)
        print(f"USD to GBP rate: {rate_usd_gbp}")

        print("Transforming data...")
        df = pd.json_normalize(products)
        
        rename_map = {
            "id": "product_id",
            "title": "title",
            "price": "price_usd",
            "category": "category_name",
            "rating.rate": "rating",
            "rating.count": "rating_count",
            "description": "description"
        }
        df = df.rename(columns=rename_map)

        df["price_gbp"] = df["price_usd"].astype(float) * rate_usd_gbp
        df["expensive"] = df["price_gbp"] > PRICE_EXPENSIVE_THRESHOLD_GBP
        df["price_band"] = pd.cut(df["price_gbp"], bins=[-1, 50, 150, 1e6],
                                  labels=["budget", "mid", "premium"])
        df["ingested_at"] = pd.Timestamp.utcnow()
        df["processing_date"] = utc_now_date_str()
        df["exchange_rate_used"] = rate_usd_gbp

        print("Transformation summary:")
        print(f"  Total products: {len(df)}")
        print(f"  Price range (GBP): {df['price_gbp'].min():.2f} - {df['price_gbp'].max():.2f}")
        print(f"  Expensive items (>{PRICE_EXPENSIVE_THRESHOLD_GBP} GBP): {df['expensive'].sum()}")

        out_path = f"processed/products/date={utc_now_date_str()}/products_{utc_now_ts_str()}.parquet"
        print(f"Saving to: {out_path}")
        
        parquet_buf = BytesIO()
        df.to_parquet(parquet_buf, index=False, engine='pyarrow')
        parquet_buf.seek(0)

        container.upload_blob(name=out_path, data=parquet_buf, overwrite=True)
        print(f"Successfully uploaded transformed data")

        result = {
            "processed_blob": out_path, 
            "fx_rate": rate_usd_gbp, 
            "count": len(df),
            "expensive_count": int(df['expensive'].sum()),
            "categories": len(df['category_name'].unique())
        }
        
        print("Transformation completed successfully!")
        return result

    except Exception as e:
        print(f"Transformation failed: {e}")
        import traceback
        traceback.print_exc()
        raise

# if __name__ == "__main__":
#     print("Running transformation script...")
    
#     try:
#         result = transform_to_parquet()
#         print("\nSUCCESS! Transformation completed:")
#         for key, value in result.items():
#             print(f"  {key}: {value}")
#     except Exception as e:
#         print(f"\nFAILED! Error: {e}")
