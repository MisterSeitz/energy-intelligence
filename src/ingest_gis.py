
import os
import glob
import json
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client
import geopandas as gpd
import pandas as pd
from shapely.geometry import mapping, Point, MultiPolygon, Polygon

# Load environment variables
load_dotenv('.env.local')

# Supabase setup
SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing Supabase credentials in .env.local")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

ESKOM_DIR = Path("Eskom")

def ingest_grid_zone(filepath: Path, zone_type: str):
    print(f"Reading Shapefile: {filepath}...")
    try:
        gdf = gpd.read_file(filepath)
        
        # Reproject to WGS84
        if gdf.crs != "EPSG:4326":
           gdf = gdf.to_crs("EPSG:4326")

        print(f"  Found {len(gdf)} features.")
        successful = 0
        
        for index, row in gdf.iterrows():
            properties = row.drop('geometry').to_dict()
            name = properties.get('NAME') or properties.get('Name') or properties.get('ZONE_NAME') or properties.get('AREA_NAME') or f"{zone_type} {index}"
            
            row_geom = row.geometry
            if row_geom is None: continue
            if not row_geom.is_valid: row_geom = row_geom.buffer(0)

            # Ensure MultiPolygon for consistency if mix of Poly/Multi
            if isinstance(row_geom, Polygon):
                row_geom = MultiPolygon([row_geom])

            wkt = row_geom.wkt
            
            payload = {
                "name": str(name),
                "zone_type": zone_type,
                "metadata": json.loads(json.dumps(properties, default=str)),
                # Insert as WKT string, expecting PostGIS casting or use raw SQL
                "geom": f"SRID=4326;{wkt}" 
            }
            
            try:
                # Using the new table that supports Polygons
                res = supabase.schema("geo_intelligence").table("grid_zones").insert(payload).execute()
                successful += 1
            except Exception as e:
                print(f"    Failed to insert {name}: {e}")
                
        print(f"  Ingested {successful}/{len(gdf)} zones.")

    except Exception as e:
        print(f"Failed to process {filepath}: {e}")

def extract_power_stations(xlsx_path: Path):
    print(f"Checking for Power Stations in {xlsx_path}...")
    try:
        # Load sheets to check for coords
        xls = pd.ExcelFile(xlsx_path)
        print(f"  Sheets: {xls.sheet_names}")
        
        # Heuristic: Check common sheets or all for 'Latitude'/'Longitude'
        for sheet in xls.sheet_names:
            try:
                df = pd.read_excel(xlsx_path, sheet_name=sheet, nrows=5000)
                cols = [c.lower() for c in df.columns]
                
                # Check for lat/long columns
                if any("lat" in c for c in cols) and any("long" in c for c in cols):
                    print(f"  Found potential coordinates in sheet '{sheet}'!")
                    
                    # Normalize columns
                    lat_col = next(c for c in df.columns if "lat" in c.lower())
                    lon_col = next(c for c in df.columns if "long" in c.lower())
                    name_col = next((c for c in df.columns if "name" in c.lower() or "station" in c.lower()), None)
                    
                    if not name_col:
                        print("    No component name column found, skipping.")
                        continue
                        
                    successful = 0
                    for _, row in df.iterrows():
                        try:
                            lat = float(row[lat_col])
                            lon = float(row[lon_col])
                            name = str(row[name_col])
                            
                            if pd.isna(lat) or pd.isna(lon): continue

                            # Construct Point WKT
                            point = Point(lon, lat)
                            wkt = point.wkt
                            
                            payload = {
                                "name": name,
                                "type": "power_station", # Normalized to snake_case
                                "geocoding_source": "Eskom GCCA Report",
                                "geom": f"SRID=4326;{wkt}",
                                "metadata": json.loads(json.dumps(row.to_dict(), default=str))
                            }
                            
                            # Insert into locations (Point table)
                            # Schema is geo_intelligence, table is locations.
                            supabase.schema("geo_intelligence").table("locations").insert(payload).execute()
                            successful += 1
                        except Exception as ex:
                            continue
                            
                    print(f"  Ingested {successful} Power Stations from '{sheet}'")
            except Exception as e:
                pass

    except Exception as e:
        print(f"Failed to read Excel report: {e}")

def main():
    if not ESKOM_DIR.exists():
        print(f"Directory {ESKOM_DIR} does not exist.")
        return

    # 1. Grid Zones (Polygons -> grid_zones table)
    local_areas = list(ESKOM_DIR.rglob("*LOCAL_AREA*.shp"))
    for f in local_areas: ingest_grid_zone(f, "Grid Zone")

    mts_zones = list(ESKOM_DIR.rglob("*MTS_ZONES*.shp"))
    for f in mts_zones: ingest_grid_zone(f, "MTS Zone")

    supply_areas = list(ESKOM_DIR.rglob("*SUPPLY_AREA*.shp"))
    for f in supply_areas: ingest_grid_zone(f, "Supply Area")

    # 2. Power Stations (Points -> locations table)
    # Use dynamic globbing to find the report
    reports = list(ESKOM_DIR.rglob("*GCCA_2025_Results_Report.xlsx*"))
    if reports:
        report_path = reports[0]
        print(f"Found report at: {report_path}")
        extract_power_stations(report_path)
    else:
        print("Report *GCCA_2025_Results_Report.xlsx* not found in Eskom directory.")

if __name__ == "__main__":
    main()
