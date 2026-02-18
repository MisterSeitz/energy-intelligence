# Input Schema

The **Energy Intelligence Actor** accepts the following input configuration:

## Properties

### `mode` (String) - **Required**
Determines the operation mode of the actor.
- **Default:** `status`
- **Allowed Values:**
  - `status`: Checks current Eskom Loadshedding status and Power Alert level.
  - `hourly`: Ingests historical hourly grid statistics (Demand, Generation, etc.) into Supabase.
  - `weekly`: Ingests weekly unplanned outage statistics into Supabase.
  - `all`: Runs both hourly and weekly ingestion.

### `local_reference` (Boolean)
- **Default:** `false`
- **Description:** If set to `true`, the actor will attempt to read CSV files from a tailored local `reference/` directory instead of downloading from the live Eskom portal. Useful for debugging or restricted environments.
