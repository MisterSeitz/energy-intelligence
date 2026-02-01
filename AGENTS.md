# Visita Energy Intelligence Actor ⚡

> **SYSTEM INSTRUCTION FOR AI AGENTS:**
> This file is the **Source of Truth** for the Energy Intelligence Actor.
> It defines the **Purpose**, **Architecture**, **Data Flows**, and **Operational Rules** for this specific microservice.
> Read this before modifying any code in this repository.

---

## 1. Identity & Mandate

**We process reality to find better ways for humanity to cooperate.**

This actor is responsible for the **Energy Intelligence** vertical of the Visita Platform. Its mandate is to provide accurate, real-time, and historical data regarding the South African power grid (Eskom Loadshedding).

### Core Principles (The Soul)
*   **Truth over Speed:** We do not guess the power status. We verify it.
*   **Data Sovereignty:** All data is stored in our Supabase architecture (`ai_intelligence` and `geo_intelligence` schemas).
*   **No Hallucinations:** AI is a utility for formatting and summarizing, not for generating facts. The raw status (Stage 4) must be exact.

---

## 2. Technical Architecture

This actor operates as a **Backend Scraper & Ingestor**. It does **NOT** handle user-facing AI generation (briefings), which is delegated to the Frontend (Next.js) to ensure fresh context.

### System Components

#### A. The Live Monitor (`src/daily_power_actor.py`)
*   **Role:** The Heartbeat.
*   **Frequency:** Hourly (Every 60 minutes).
*   **Target:** 
    1. `loadshedding.eskom.co.za` (Loadshedding Stage)
    2. `poweralert.co.za` (Grid Health Color: Green/Gold/Black/Red)
*   **Output:** Upserts to `ai_intelligence.power_alerts`.
*   **Logic:**
    1.  Fetches `https://loadshedding.eskom.co.za/` for Stage (0-8).
    2.  Fetches `https://www.poweralert.co.za/PowerAlertAPI/api/PowerAlertForecast/CurrentSystemStatus` for Color.
    3.  Upserts the combined state to Supabase.

#### B. The Static Ingestors (`src/ingest_*.py`)
These are "One-Off" or "Infrequent" scripts used to seed the reference data.
*   `src/ingest_schedule.py`: Ingests the Master Spreadsheet of loadshedding schedules into `geo_intelligence.loadshedding_schedule`.
*   `src/ingest_gis.py`: Processes GeoJSON/Shapefiles to map Suburbs to Blocks (`geo_intelligence.loadshedding_suburbs`) and ingest Power Station locations (`geo_intelligence.locations`).

#### C. The Database (Supabase)
We write to the following authoritative tables:

| Schema | Table | Purpose | Source |
| :--- | :--- | :--- | :--- |
| `ai_intelligence` | `power_alerts` | Live Status (Stage, Text) | `daily_power_actor.py` |
| `geo_intelligence` | `loadshedding_schedule` | The Static Schedule (Time vs Stage) | `ingest_schedule.py` |
| `geo_intelligence` | `loadshedding_suburbs` | Suburb -> Block Mapping | `ingest_gis.py` |
| `geo_intelligence` | `locations` | Power Station Coordinates | `ingest_gis.py` |

---

## 3. Data Flow & Logic

### The "Loop"
1.  **Actor** wakes up.
2.  **Actor** checks Eskom Live Status.
3.  **Actor** writes `Stage X` to `ai_intelligence.power_alerts`.
4.  **Frontend** (on user visit) reads `Stage X`.
5.  **Frontend** calculates `User Block` via `geo_intelligence.loadshedding_suburbs`.
6.  **Frontend** derives "Do I have power?" by checking `Stage X` vs `Schedule` for `User Block`.
7.  **Frontend** generates AI Briefing ("Stage 4 is active, expect outage at 18:00").

### Critical Logic: Power Station Linking
To link a Ward to its Power Station (for "My Grid" features), we use PostGIS spatial queries:

```sql
SELECT w.ward_code, l.name as power_station
FROM geo_intelligence.locations l
JOIN geo_intelligence.wards w
ON ST_Distance(w.geom::geography, l.geom::geography) < 50000
WHERE l.type = 'power_station'
ORDER BY ST_Distance(w.geom::geography, l.geom::geography) ASC
LIMIT 1;
```

---

## 4. Development Guidelines

### Codebase Structure
```
actors/intelligence/energy-intelligence/
├── .actor/                 # Apify Config
├── src/
│   ├── daily_power_actor.py # MAIN ENTRY POINT (Live)
│   ├── ingest_eskom.py     # Reference Data Ingestor
│   ├── ingest_gis.py       # Spatial Data Ingestor
│   ├── ingest_schedule.py  # Schedule CSV Ingestor
│   └── main.py             # Actor Wrapper
├── AGENTS.md               # Source of Truth (This File)
├── GEMINI.md               # Platform "Soul"
└── IDE_CONTEXT.md          # Frontend/IDE Handoff Context
```

### Environment Variables
The actor requires the following secrets (set in Apify Console or `.env` locally):
*   `SUPABASE_URL`: Connection URL.
*   `SUPABASE_KEY`: Service Role Key (for writing to `ai_intelligence`).

### Deployment
*   **Platform:** Apify
*   **Docker:** Uses standard Python image.
*   **Command:** `apify push`

---

## 5. Maintenance & Debugging

*   **Ingestion Failures:** If `daily_power_actor` fails, the `power_alerts` table becomes stale. The Frontend should handle "Stale Data" (curr_time - updated_at > 2 hours) by showing a warning.
*   **Schedule Changes:** If Eskom changes the block configuration, `ingest_gis.py` and `ingest_schedule.py` must be re-run with new reference data.
