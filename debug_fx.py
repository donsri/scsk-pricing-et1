from etl.transform import _blob_service, _download_json_blob
from etl.config import AZURE_STORAGE_CONTAINER

# Download and inspect the FX data
blob_service = _blob_service()
container = blob_service.get_container_client(AZURE_STORAGE_CONTAINER)

fx_data = _download_json_blob(container, "raw/fx/date=2025-09-03/fx_20250903_142456.json")
print("FX data structure:")
print(fx_data)
print("\nKeys in FX data:")
print(list(fx_data.keys()) if isinstance(fx_data, dict) else "Not a dict")
