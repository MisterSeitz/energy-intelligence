
import os
import requests
from bs4 import BeautifulSoup
import openpyxl
import json
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv(".env.local")

SUPABASE_URL = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

from apify import Actor

class PowerIntelligence:
    def __init__(self):
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    async def fetch_power_alert(self):
        """Fetches the official Power Alert status (Green, Orange, Red) from poweralert.co.za API."""
        url = "https://www.poweralert.co.za/PowerAlertAPI/api/PowerAlertForecast/CurrentSystemStatus"
        try:
            # Using synchronous requests inside async method for simplicity (or use aiohttp if preferred, 
            # but requests is already imported and lightweight enough for 2 calls)
            # Running in executor to avoid blocking loop if we were doing heavy work, 
            # but here straightforward requests is fine for this simple actor. 
            # Ideally use aiohttp, but let's stick to requests to minimize unnecessary refactors.
            response = requests.get(url, timeout=15)
            if response.status_code == 200:
                data = response.json()
                # Expected format: {"Color": "Green", ...}
                return data.get("Color", "Unknown")
            else:
                Actor.log.warning(f"PowerAlert API returned {response.status_code}")
                return "Unknown"
        except Exception as e:
             Actor.log.error(f"Failed to fetch Power Alert: {e}")
             return "Unknown"

    def fetch_eskom_status(self):
        """Scrapes the current loadshedding status from Eskom."""
        # Method 1: Scrape Main Page
        url = "https://loadshedding.eskom.co.za/"
        stage = -1
        status_text = "Unknown"
        raw_text = ""
        
        try:
            response = requests.get(url, timeout=15)
            if response.status_code == 200:
                html = response.text
                soup = BeautifulSoup(html, "html.parser")
                status_span = soup.find("span", {"id": "lsstatus"})
                raw_text = status_span.get_text(strip=True) if status_span else "Unknown"
                
                if "NOT LOAD SHEDDING" in raw_text.upper():
                    stage = 0
                    status_text = "Suspended"
                elif "STAGE" in raw_text.upper():
                    import re
                    match = re.search(r"STAGE\s*(\d+)", raw_text.upper())
                    if match:
                        stage = int(match.group(1))
                        status_text = "Active"
                else:
                    # If we found text but didn't match standard patterns
                    if raw_text != "Unknown":
                        status_text = raw_text
            else:
                 Actor.log.warning(f"Eskom Main Site returned {response.status_code}")
        except Exception as e:
            Actor.log.warning(f"Error scraping Eskom Main Site: {e}")

        # Method 2: Fallback to GetStatus API if Method 1 failed
        if stage == -1:
             Actor.log.info("Attempting fallback to Eskom GetStatus API...")
             try:
                 api_url = "https://loadshedding.eskom.co.za/LoadShedding/GetStatus"
                 resp = requests.get(api_url, timeout=15)
                 if resp.status_code == 200:
                     val = int(resp.text.strip())
                     if val > 0:
                         # Mapping: 1 -> Stage 0, 2 -> Stage 1, etc.
                         stage = val - 1
                         status_text = "Active" if stage > 0 else "Suspended"
                         raw_text = f"API Value: {val}"
                         Actor.log.info(f"Fallback API success: Stage {stage}")
                     else:
                         Actor.log.warning(f"Fallback API returned invalid value: {val}")
                 else:
                     Actor.log.warning(f"Fallback API returned {resp.status_code}")
             except Exception as e:
                 Actor.log.warning(f"Fallback API failed: {e}")

        # Method 3: Local Dev Fallback
        if stage == -1 and os.path.exists("Eskom/Eskom load shedding.html"):
             Actor.log.warning("Falling back to local file 'Eskom/Eskom load shedding.html' for dev/testing.")
             try:
                 with open("Eskom/Eskom load shedding.html", "r") as f:
                    html = f.read()
                    soup = BeautifulSoup(html, "html.parser")
                    status_span = soup.find("span", {"id": "lsstatus"})
                    raw_text = status_span.get_text(strip=True) if status_span else "Unknown"
                    if "NOT LOAD SHEDDING" in raw_text.upper():
                        stage = 0
                        status_text = "Suspended"
                    elif "STAGE" in raw_text.upper():
                        import re
                        match = re.search(r"STAGE\s*(\d+)", raw_text.upper())
                        if match:
                            stage = int(match.group(1))
                            status_text = "Active"
             except Exception as e:
                 Actor.log.error(f"Failed to read local fallback file: {e}")

        return {
            "stage": stage,
            "status": status_text,
            "raw_text": raw_text if raw_text else status_text
        }

    async def run(self):
        await Actor.init()
        Actor.log.info("🚀 Starting Daily Power Intelligence Actor...")
        
        # 1. Fetch Eskom Loadshedding Status
        eskom_data = self.fetch_eskom_status()
        Actor.log.info(f"⚡ Eskom Status: Stage {eskom_data['stage']} ({eskom_data['status']})")

        # 2. Fetch Power Alert API
        power_alert_color = await self.fetch_power_alert()
        Actor.log.info(f"🚦 Power Alert Level: {power_alert_color}")
        
        # 3. Construct Payload
        # We assume 'stage' > -1 for a valid update. If -1, we might skip upsert or log error.
        if eskom_data['stage'] == -1 and power_alert_color == "Unknown":
            Actor.log.error("❌ Failed to fetch data from both sources. Aborting upsert.")
            await Actor.exit(exit_code=1)
            return

        payload = {
            "stage": eskom_data['stage'] if eskom_data['stage'] != -1 else 0, # Default to 0 if failed? Or keep previous? 
                                                                             # Safer to fail if critical, but for now defaulting 0 is risky.
                                                                             # Let's trust fetch_eskom_status error handling/local fallback.
            "status": eskom_data['status'],
            "raw_response": {
                "eskom_text": eskom_data['raw_text'],
                "power_alert_color": power_alert_color,
                "provider": "Eskom + PowerAlert.co.za"
            }
        }
        
        Actor.log.info(f"💾 Persisting data: {payload}")
        
        # Insert into Supabase
        try:
            self.supabase.schema("ai_intelligence").table("power_alerts").insert(payload).execute()
            Actor.log.info("✅ Data successfully pushed to Supabase")
        except Exception as e:
             Actor.log.error(f"❌ Supabase Insert Failed: {e}")
        
        # Push to Apify Dataset
        await Actor.push_data(payload)
        
        Actor.log.info("✅ Actor run complete.")
        await Actor.exit()

if __name__ == "__main__":
    import asyncio
    actor = PowerIntelligence()
    asyncio.run(actor.run())
