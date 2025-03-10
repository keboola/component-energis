# 🔧 Energis Component Configuration

This component allows you to retrieve energy data from the Energis API, process it based on defined criteria, and store 
it in Keboola Storage. Configure the authentication, data selection, and granularity settings as needed.

## Authentication Settings

> **Required:** ✅ Yes  
> **Description:** Provides API credentials for authentication.

| **Section**        | **Required** | **Description** |
|--------------------|-------------|---------------|
| **Authentication** | ✅ Yes       | Provides API credentials for authentication. |
| **Sync Options**   | ✅ Yes       | Defines data extraction settings, including dataset, nodes, and date range. |
| **Debug Mode**     | ❌ No         | Enables or disables debug logging for troubleshooting. |

## Synchronization Options

> **Required:** ✅ Yes  
> **Description:** Define the dataset, time range, and data granularity for extraction.

| **Property**                      | **Required** | **Type**     | **Default** | **Description** |
|-----------------------------------|--------------|------------|------------|---------------|
| **authentication.username**       | ✅ Yes        | String | _(None)_ | Username for API authentication. |
| **authentication.#password**      | ✅ Yes        | String (password) | _(None)_ | Password for API authentication. |
| **authentication.environment**    | ✅ Yes        | Enum (`dev` / `prod`) | `prod` | Selects the API environment (development or production). |
| **sync_options.dataset**          | ✅ Yes        | Enum (`xexport`) | `xexport` | Specifies the dataset for extraction. |
| **sync_options.nodes**            | ✅ Yes        | Array of Integers | `[]` | List of node IDs for data retrieval. |
| **sync_options.date_from**        | ✅ Yes        | Date (`YYYY-MM-DD`) | `2024-12-01` | Start date for data extraction. |
| **sync_options.date_to**          | ❌ No         | Date (`YYYY-MM-DD`) | _(Today)_ | End date for data extraction. If not set, defaults to today. |
| **sync_options.granularity**      | ✅ Yes        | Enum (`year`, `quarterYear`, `month`, `day`, `hour`, `quarterHour`, `minute`) | `day` | Defines data granularity for extraction. |
| **sync_options.reload_full_data** | ❌ No | Boolean| _False_      | When enabled, retrieves the complete dataset from 'date_from', bypassing incremental loading. |

## Debug Mode

> **Required:** ❌ No  
> **Description:** Enables debug logging for troubleshooting.
 
| **Property**        | **Required** | **Type**     | **Default** | **Description** |
|--------------------|-----------|------------|------------|---------------|
| **debug** | ❌ Yes | Boolean | `false` | Enables debug mode for additional logging. |


## Example configuration

```json
{
  "authentication": {
    "username": "your_username",
    "#password": "your_password",
    "environment": "prod"
  },
  "sync_options": {
    "dataset": "xexport",
    "nodes": [7090001, 7090002],
    "date_from": "2024-01-01",
    "date_to": "2024-12-31",
    "granularity": "hour"
  },
  "debug": false
}
```