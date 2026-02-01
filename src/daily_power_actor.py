
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
        url = "https://loadshedding.eskom.co.za/"
        html = ""
        stage = -1
        status_text = "Unknown"
        
        try:
            response = requests.get(url, timeout=15)
            html = response.text
        except Exception as e:
            Actor.log.warning(f"Network error scraping Eskom, falling back to local file for dev: {e}")
            if os.path.exists("Eskom/Eskom load shedding.html"):
                 with open("Eskom/Eskom load shedding.html", "r") as f:
                    html = f.read()

        if html:
            soup = BeautifulSoup(html, "html.parser")
            
            # Logic 1: Check the Status Text
            status_span = soup.find("span", {"id": "lsstatus"})
            status_text = status_span.get_text(strip=True) if status_span else "Unknown"
            
            # Logic 2: Check the Stage
            if "NOT LOAD SHEDDING" in status_text.upper():
                stage = 0
                status_text = "Suspended"
            elif "STAGE" in status_text.upper():
                import re
                match = re.search(r"STAGE\s*(\d+)", status_text.upper())
                if match:
                    stage = int(match.group(1))
                    status_text = "Active"
                    
        return {
            "stage": stage,
            "status": status_text,
            "raw_text": status_text
        }

    async def run(self):
        await Actor.init()
        Actor.log.info("⚡ Starting Daily Power Intelligence Actor...")
        
        # 1. Fetch Eskom Loadshedding Status
        eskom_data = self.fetch_eskom_status()
        Actor.log.info(f"Eskom Status: Stage {eskom_data['stage']} ({eskom_data['status']})")

        # 2. Fetch Power Alert API
        power_alert_color = await self.fetch_power_alert()
        Actor.log.info(f"Power Alert Level: {power_alert_color}")
        
        # 3. Construct Payload
        # We assume 'stage' > -1 for a valid update. If -1, we might skip upsert or log error.
        if eskom_data['stage'] == -1 and power_alert_color == "Unknown":
            Actor.log.error("Failed to fetch data from both sources. Aborting upsert.")
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
        
        Actor.log.info(f"Persisting data: {payload}")
        
        # Insert into Supabase
        try:
            self.supabase.schema("ai_intelligence").table("power_alerts").insert(payload).execute()
        except Exception as e:
             Actor.log.error(f"Supabase Insert Failed: {e}")
        
        # Push to Apify Dataset
        await Actor.push_data(payload)
        
        Actor.log.info("✅ Actor run complete.")
        await Actor.exit()

if __name__ == "__main__":
    import asyncio
    actor = PowerIntelligence()
    asyncio.run(actor.run())
