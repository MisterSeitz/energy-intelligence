import os
import argparse
import pandas as pd
from dotenv import load_dotenv
# from supabase import create_client, Client # Removed due to installation issues
from io import StringIO
import requests
import json
from datetime import datetime

# Load environment variables
# Try loading from current directory or parent (if running from src)
load_dotenv('.env.local')
load_dotenv('../.env.local')

SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL") # Updated to match provided key
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    # Debug print to help identify what is missing
    print(f"DEBUG: URL={SUPABASE_URL}, KEY={SUPABASE_KEY[:5]}..." if SUPABASE_KEY else "KEY=None")
    raise ValueError("Missing Supabase credentials (SUPABASE_SERVICE_ROLE_KEY/NEXT_PUBLIC_SUPABASE_URL) in .env.local")

# Supabase REST API Headers
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates", # Upsert behavior
    "Content-Profile": "ai_intelligence", # Target schema for write
    "Accept-Profile": "ai_intelligence"   # Target schema for read
}

# supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY) 


# URL Configuration
ESKOM_ROOT = "https://www.eskom.co.za/dataportal/wp-content/uploads/2026/02"
# Fallback to local reference if needed, but default to URLs as requested
LOCAL_REF = "reference"

URLS = {
    "hourly_demand": f"{ESKOM_ROOT}/System_hourly_actual_and_forecasted_demand.csv",
    "station_build_up": f"{ESKOM_ROOT}/Station_Build_Up.csv",
    "hourly_gen": f"{ESKOM_ROOT}/Hourly_Generation.csv",
    "weekly_outages": f"{ESKOM_ROOT}/Weekly_unplanned_outages.csv",
    "ocgt_factors": f"{ESKOM_ROOT}/Financial_year_load_factor_OCGT.csv",
}

FILE_MAP = {
    "hourly_demand": "supply_side/System_hourly_actual_and_forecasted_demand.csv",
    "station_build_up": "demand_side/Station_Build_Up.csv",
    "hourly_gen": "renewables/Hourly_Generation.csv",
    "weekly_outages": "outages/Weekly_unplanned_outages.csv",
    "ocgt_factors": "ocgt_usage/Financial_year_load_factor_OCGT.csv"
}

def fetch_csv_as_df(url_key, local_root=None):
    """
    Fetches CSV from URL or reads from local path.
    """
    # Try local first if root provided
    if local_root:
        rel_path = FILE_MAP.get(url_key)
        if rel_path:
            local_file = os.path.join(local_root, rel_path)
            if os.path.exists(local_file):
                print(f"📂 Reading local file: {local_file}")
                return pd.read_csv(local_file)
            else:
                print(f"⚠️ Local file not found: {local_file}, trying URL...")
    
    url = URLS.get(url_key)
    if not url:
        raise ValueError(f"Unknown URL key: {url_key}")
        
    print(f"🌐 Fetching URL: {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        csv_content = StringIO(response.text)
        return pd.read_csv(csv_content)
    except Exception as e:
        print(f"❌ Error fetching {url}: {e}")
        # Only retry local if we haven't already (e.g. strict URL mode but failed)
        # But here logic is: if local_root was passed we tried it first.
        # If not passed, we can try default 'reference' as fallback?
        # Let's keep it simple: strict URL unless local_root passed.
        if not local_root:
             print("   (Use --local-reference to try local files)")
        raise e

def clean_column_names(df):
    df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('+', '_').str.replace('/', '_')
    return df

def upsert_grid_stats(local_root=None):
    print("🚀 Starting Grid Stats Ingestion (Hourly)...")
    
    # 1. Fetch Data
    try:
        df_demand = fetch_csv_as_df("hourly_demand", local_root)
        df_station = fetch_csv_as_df("station_build_up", local_root)
        df_gen = fetch_csv_as_df("hourly_gen", local_root) # Added hourly_gen for Renewables
    except Exception as e:
        print(f"Aborting grid stats: {e}")
        return
    
    # 2. Standardize Timestamps
    df_demand = clean_column_names(df_demand)
    df_station = clean_column_names(df_station)
    df_gen = clean_column_names(df_gen)

    # Rename to 'timestamp'
    for df in [df_demand, df_station, df_gen]:
        if 'DateTimeKey' in df.columns:
            df.rename(columns={'DateTimeKey': 'timestamp'}, inplace=True)
        elif 'Date_Time_Hour_Beginning' in df.columns:
             df.rename(columns={'Date_Time_Hour_Beginning': 'timestamp'}, inplace=True)
        
        # Convert to datetime
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])

    # 3. Merge
    # Merge Demand + Station
    merged = pd.merge(df_demand, df_station, on='timestamp', how='outer')
    # Merge + Gen
    merged = pd.merge(merged, df_gen, on='timestamp', how='outer')
    
    # 4. Prepare Payload
    records = []
    for _, row in merged.iterrows():
        def get_val(col):
            val = row.get(col)
            return float(val) if pd.notnull(val) else None
        
        ts = row.get('timestamp')
        if pd.isnull(ts): continue

        record = {
            "timestamp": ts.isoformat(),
            "residual_demand": get_val('Residual_Demand'),
            "rsa_contracted_forecast": get_val('RSA_Contracted_Forecast'),
            "thermal_generation": get_val('Thermal_Gen_Excl_Pumps_and_GT'),
            "nuclear_generation": get_val('Nuclear_Generation'),
            "pumped_storage_gen": get_val('Pumped_Storage_Gen'),
            "wind": get_val('Wind'),
            "pv": get_val('PV'),
            "csp": get_val('CSP'),
            "other_re": get_val('Other_RE'),
        }
        records.append(record)
            
    print(f"Prepared {len(records)} records for upsert.")
    
    # 5. Batch Upsert
    BATCH_SIZE = 1000
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        try:
            url = f"{SUPABASE_URL}/rest/v1/grid_stats"
            response = requests.post(url, json=batch, headers=HEADERS)
            response.raise_for_status()
            print(f"   Upserted batch {i//BATCH_SIZE + 1}/{(len(records)//BATCH_SIZE)+1}")
        except Exception as e:
            print(f"   ❌ Error upserting batch {i}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"      Response: {e.response.text}")

def upsert_weekly_outages(local_root=None):
    print("🚀 Starting Weekly Outages Ingestion...")
    try:
        df = fetch_csv_as_df("weekly_outages", local_root)
    except Exception as e:
        print(f"Aborting weekly outages: {e}")
        return

    df = clean_column_names(df)
    
    # 'Week_min_DateKey' -> week_start_date
    if 'Week_min_DateKey' in df.columns:
        df.rename(columns={'Week_min_DateKey': 'week_start_date'}, inplace=True)
        
    df['week_start_date'] = pd.to_datetime(df['week_start_date'])
    
    records = []
    for _, row in df.iterrows():
        def get_val(col):
            val = row.get(col)
            return float(val) if pd.notnull(val) else None
            
        record = {
            "week_start_date": row['week_start_date'].date().isoformat(),
            "uclf": get_val('Average_of_UCLF_OCLF'), 
            "oclf": 0, 
            "planned_maintenance": 0, 
            "unplanned_outages": get_val('Max_of_UCLF_OCLF') 
        }
        records.append(record)

    if records:
        try:
            url = f"{SUPABASE_URL}/rest/v1/outages_weekly"
            response = requests.post(url, json=records, headers=HEADERS)
            response.raise_for_status()
            print(f"✅ Upserted {len(records)} outage records.")
        except Exception as e:
            print(f"❌ Error upserting outages: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"      Response: {e.response.text}")

def main():
    parser = argparse.ArgumentParser(description="Ingest Eskom Data")
    parser.add_argument("--mode", choices=["hourly", "weekly", "all"], default="all")
    parser.add_argument("--local-reference", action="store_true", help="Use local reference files")
    args = parser.parse_args()
    
    local_root = LOCAL_REF if args.local_reference else None

    if args.mode in ["hourly", "all"]:
        upsert_grid_stats(local_root)
    
    if args.mode in ["weekly", "all"]:
        upsert_weekly_outages(local_root)

if __name__ == "__main__":
    main()
