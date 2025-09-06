# etl/utils.py
from datetime import datetime, timezone

def utc_now_date_str() -> str:
    """Return current UTC date as YYYY-MM-DD string"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def utc_now_ts_str() -> str:
    """Return current UTC timestamp as YYYYMMDD_HHMMSS string"""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

def utc_now_iso() -> str:
    """Return current UTC timestamp as ISO format string"""
    return datetime.now(timezone.utc).isoformat()

# if __name__ == "__main__":
#     print(f"Date: {utc_now_date_str()}")
#     print(f"Timestamp: {utc_now_ts_str()}")
#     print(f"ISO: {utc_now_iso()}")