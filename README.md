# **Energis Extractor**
=============

## **Description**
The **Energis Extractor** retrieves energy-related data from the **Energis API** and loads it into **Keboola Connection** for further analysis. It supports **incremental loading**, **various data granularities**, and **date range filtering** to allow precise data extraction.

---

## **Table of Contents**
- [Functionality Notes](#functionality-notes)
- [Prerequisites](#prerequisites)
- [Features](#features)
- [Supported Endpoints](#supported-endpoints)
- [Configuration](#configuration)
  - [Authentication Settings](#authentication-settings)
  - [Synchronization Options](#synchronization-options)
  - [Debug Mode](#debug-mode)
- [Output](#output)
- [Development](#development)
  - [Running Locally](#running-locally)
  - [Running Tests](#running-tests)
- [Integration](#integration)

---

## **Functionality Notes**
- Extracts **energy consumption and related metrics** from Energis.
- Supports **incremental data fetching** to avoid duplicate records.
- Allows **date-based filtering** to control extracted data ranges.
- Provides **structured output tables** ready for analysis.

---

## **Prerequisites**
Before using this component, ensure that:
1. You have **valid API credentials** for the Energis system.
2. Your user account has **appropriate permissions** to access required datasets.
3. You have registered the **Keboola Connection application** (if required).

---

## **Features**
| **Feature**             | **Description**                               |
|-------------------------|-----------------------------------------------|
| **Dynamic UI Form**     | Easy configuration via a user-friendly form. |
| **Dataset Selection**   | Supports different datasets for extraction.  |
| **Incremental Loading** | Avoids re-fetching already extracted data.   |
| **Backfill Mode**       | Allows historical data retrieval.            |
| **Granularity Options** | Extracts data at multiple time resolutions.  |
| **Date Range Filter**   | Enables precise time-based filtering.        |

---

## **Supported Endpoints**
This extractor currently supports the **`xexport` dataset**. If you require additional endpoints, submit your request at [ideas.keboola.com](https://ideas.keboola.com/).

---

## **Configuration**
For a full breakdown of configuration options, refer to the [Configuration Documentation](#).

### **Authentication Settings**
| **Property**        | **Required** | **Type**     | **Default** | **Description** |
|--------------------|------------|------------|------------|---------------|
| `authentication.username` | ✅ Yes | String | _(None)_ | Username for API authentication. |
| `authentication.#password` | ✅ Yes | String (password) | _(None)_ | Password for API authentication. |
| `authentication.environment` | ✅ Yes | Enum (`dev` / `prod`) | `prod` | Selects the API environment (development or production). |

### **Synchronization Options**
| **Property**        | **Required** | **Type**     | **Default** | **Description** |
|--------------------|------------|------------|------------|---------------|
| `sync_options.dataset` | ✅ Yes | Enum (`xexport`) | `xexport` | Specifies the dataset for extraction. |
| `sync_options.nodes` | ✅ Yes | Array of Integers | `[]` | List of node IDs for data retrieval. |
| `sync_options.date_from` | ✅ Yes | Date (`YYYY-MM-DD`) | `2024-12-01` | Start date for data extraction. |
| `sync_options.date_to` | ❌ No | Date (`YYYY-MM-DD`) | _(Today)_ | End date for data extraction. If not set, defaults to today. |
| `sync_options.granularity` | ✅ Yes | Enum (`year`, `quarterYear`, `month`, `day`, `hour`, `quarterHour`, `minute`) | `day` | Defines data granularity for extraction. |

### **Debug Mode**
| **Property** | **Required** | **Type** | **Default** | **Description** |
|-------------|--------------|--------|------------|---------------|
| `debug` | ❌ No         | Boolean | `false` | Enables debug mode for additional logging. |

---

## **Output**
The extracted data is stored in **CSV tables** within **Keboola Storage**. Each dataset includes structured fields with **timestamps, node identifiers, and recorded values**.

For the exact output schema, refer to the [Output Schema Documentation](#).

---

## **Development**
### **Running Locally**
To customize the local data folder path, modify the `docker-compose.yml` file:

```yaml
volumes:
  - ./:/code
  - ./CUSTOM_FOLDER:/data
