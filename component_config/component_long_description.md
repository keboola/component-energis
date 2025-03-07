The Energis API Extractor is a Keboola component that enables users to seamlessly retrieve and process energy consumption 
data from the Energis platform. The component is designed for efficient data extraction, supporting multiple 
levels of granularity, including:

- Yearly (`year`)
- Quarterly (`quarterYear`)
- Monthly (`month`)
- Daily (`day`)
- Hourly (`hour`)
- Quarter-hour (`quarterHour`)
- Minute (`minute`)

## 🔧 Features & Functionality

- **Secure Authentication**: Uses username/password authentication for API access.
- **Flexible Data Selection**: Users can specify nodes, time range, and granularity for precise data extraction.
- **Incremental Fetching**: The component remembers the last processed date and fetches only new data.
- **Automatic Data Parsing**: Converts API responses into a structured format, handling various timestamp formats.
- **CSV Output & Manifest Generation**: Saves data as CSV and generates metadata for seamless integration into Keboola Storage.

## 📂 Output Format

The extracted data is stored in Keboola Storage, with each dataset containing:

- **Node ID (uzel)** – Unique identifier of the energy node.
- **Value (hodnota)** – The recorded energy consumption or measurement.
- **Timestamp (cas)** – The formatted date/time of the measurement.

## 🚀 Use Cases

- **Energy Monitoring & Reporting** – Retrieve historical and real-time energy consumption data.
- **Data Integration for Analysis** – Combine Energis data with other sources in Keboola for advanced analytics.
- **Automated ETL Pipelines** – Automate data ingestion into data warehouses and BI tools.

## 🔒 Security & Logging

Sensitive credentials are **masked in logs**, ensuring security. Detailed logging allows debugging and monitoring of API requests.

This component simplifies **data extraction from Energis**, enabling automated, scalable, and structured energy data processing within Keboola.