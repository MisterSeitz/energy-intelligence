
import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import numpy as np

load_dotenv(".env.local")

SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

class ScheduleIngestor:
    def __init__(self):
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    def ingest_suburbs(self, file_path, province_id):
        print(f"Ingesting Suburbs from {file_path} for {province_id}...")
        df = pd.read_excel(file_path, sheet_name='SP_List')
        # Columns: SHEET, MP_NAME, SP_NAME, Sheet_MP... BLOCK
        # We need MP_NAME, SP_NAME, BLOCK.
        # Check column names
        # Based on previous output: 'MP_NAME', 'SP_NAME', 'BLOCK'
        
        records = []
        for _, row in df.iterrows():
            block = str(row.get('BLOCK', '')).strip()
            # Handle "3" vs "3B" etc? Assuming simple IDs for now.
            if not block or block == 'nan': continue
            
            payload = {
                "province_id": province_id, 
                "municipality_name": row.get('MP_NAME'),
                "suburb_name": row.get('SP_NAME'),
                "block_id": block,
                "metadata": {"sheet": row.get('SHEET')}
            }
            records.append(payload)

        
        # Deduplicate records based on key
        seen = set()
        unique_records = []
        for r in records:
            key = (r['province_id'], r['municipality_name'], r['suburb_name'])
            if key not in seen:
                seen.add(key)
                unique_records.append(r)

        # Batch insert
        records = unique_records
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            try:
                self.supabase.schema("geo_intelligence").table("loadshedding_suburbs").upsert(
                    batch, on_conflict="province_id, municipality_name, suburb_name"
                ).execute()
                print(f"Inserted batch {i} - {i+len(batch)}")
            except Exception as e:
                print(f"Error inserting suburbs: {e}")

    def ingest_schedule(self, file_path, province_id):
        print(f"Ingesting Schedule from {file_path} for {province_id}...")
        # Read Schedule Sheet, skipping header rows
        # Row 14 in Excel is index 13? 
        # previous `print` showed data starting at row index 0 when `skiprows=14`.
        df = pd.read_excel(file_path, sheet_name='Schedule', skiprows=14, header=None)
        
        # Iteration Logic
        # Columns: 0 (Start), 1 (End), 2 (Stage 1 label?), 3..33 (Days 1..31)
        
        schedule_records = []
        current_start = None
        current_end = None
        
        # Loop through rows
        # We expect groups of 8 rows
        # total rows approx 12 (slots) * 8 = 96 rows
        
        for idx, row in df.iterrows():
            if idx >= 96: break # Safety limit
            
            # Check for new time slot (every 8 rows)
            # Row 0 of block has times
            if not pd.isna(row[0]):
                current_start = str(row[0]) # e.g. "00:00:00"
                current_end = str(row[1])
            
            stage_level = (idx % 8) + 1
            
            # Iterate Days (Col 3 to 33)
            # Day 1 is at index 3
            for day_idx in range(1, 32):
                col_idx = day_idx + 2
                if col_idx >= len(row): break
                
                val = row[col_idx]
                if pd.isna(val) or val == 0: continue
                
                block_id = str(int(val)) # "1", "13"
                
                record = {
                    "province_id": "GP",
                    "stage": stage_level,
                    "day_of_month": day_idx,
                    "start_time": current_start,
                    "end_time": current_end,
                    "affected_blocks": [block_id] # Storing as array to allow merging if needed
                }
                
                # We could aggregate this: (Stage, Day, Time) -> [Blocks]
                # But creating individual rows per block allowing simple queries is fine?
                # Actually, `affected_blocks` suggests one row per Time/Day/Stage with list.
                # But here we are iterating stages.
                # Wait.
                # If Stage 5 includes Stage 1..5 blocks. 
                # The table should store "At Stage X, Add Block Y".
                # My `loadshedding_schedule` definition has `affected_blocks text[]`.
                # If I want to query efficiently: "Is Block 3 off?"
                # I'd prefer `block_id` column.
                
                # Let's pivot: Store SINGLE assignments.
                # Table: `loadshedding_schedule_assignments`?
                # Let's use `loadshedding_schedule` but store `block_id` as text.
                # I'll modify the ingest to store one row per block assignment.
                # But that's LOTS of rows. (31 days * 12 slots * 8 stages * 16 blocks = 47k rows).
                # That's fine for Supabase.
                
                schedule_records.append({
                    "province_id": "GP",
                    "stage": stage_level,
                    "day_of_month": day_idx,
                    "start_time": current_start,
                    "end_time": current_end,
                    "affected_blocks": [block_id] # Schema is text[], so list is correct
                })

        # Batch insert
        print(f"Total schedule records to insert: {len(schedule_records)}")
        batch_size = 2000
        for i in range(0, len(schedule_records), batch_size):
            batch = schedule_records[i:i+batch_size]
            try:
                 self.supabase.schema("geo_intelligence").table("loadshedding_schedule").insert(batch).execute()
                 print(f"Inserted schedule batch {i}")
            except Exception as e:
                print(f"Error inserting schedule: {e}")

if __name__ == "__main__":
    ingestor = ScheduleIngestor()
    base_dir = "Eskom/loadshedding_schedules"
    
    # Map Filenames (prefix) to Province IDs
    province_map = {
        "Gauteng_LS": "GP",
        "WesternCape_LS": "WC",
        "KwaZulu-Natal_LS": "KZN", # Check filename casing: Kwazulu-Natal_LS.xlsx from dir output
        "Kwazulu-Natal_LS": "KZN",
        "EasternCape_LS": "EC",
        "FreeState_LS": "FS",
        "Mpumalanga_LS": "MP",
        "Limpopo_LS": "LP",
        "NorthWest_LS": "NW",
        "NorthernCape_LS": "NC"
    }

    import glob
    files = glob.glob(os.path.join(base_dir, "*_LS.xlsx"))
    
    for file_path in files:
        filename = os.path.basename(file_path)
        prefix = filename.replace(".xlsx", "")
        
        province_id = province_map.get(prefix, "UNKNOWN")
        print(f"🌍 Processing {prefix} -> {province_id}...")
        
        ingestor.ingest_suburbs(file_path, province_id)
        ingestor.ingest_schedule(file_path, province_id)
