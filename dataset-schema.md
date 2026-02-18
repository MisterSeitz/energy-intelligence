# Dataset Schema

The default dataset will contain items with the following structure (primarily for `status` mode).

## Fields

| Field | Type | Description |
|---|---|---|
| `stage` | Integer | The current Eskom loadshedding stage (0-8). |
| `status` | String | Textual description of the status (e.g., "Suspended", "Active"). |
| `raw_response` | Object | Raw details from the source APIs. |
| `raw_response.eskom_text` | String | Original text scraped or returned from Eskom API. |
| `raw_response.power_alert_color` | String | Status color from PowerAlert.co.za (Green, Orange, Red). |
| `raw_response.provider` | String | Source attribution. |
