# Output Schema

The actor produces different outputs based on the `mode`.

## Mode: `status` (Default)
In status mode, the actor pushes a single JSON object to the default Apify Dataset with the current power status.

**Example Output:**
```json
{
  "stage": 2,
  "status": "Active",
  "raw_response": {
    "eskom_text": "Stage 2",
    "power_alert_color": "Green",
    "provider": "Eskom + PowerAlert.co.za"
  }
}
```

## Mode: `hourly`, `weekly`, `all`
In ingestion modes, the actor **does not push items to the Apify Dataset**. Instead, it performs **upsert operations** directly to the configured Supabase database.

- **Target Database:** Supabase (`ai_intelligence` schema)
- **Tables Affected:**
  - `grid_stats` (Hourly data)
  - `outages_weekly` (Weekly data)

**Logs:**
Check the run logs for details on the number of records processed and any errors encountered during Supabase ingestion.
