# HELIOS: Renewable Energy Intelligence Platform

## Project Overview

**HELIOS** is a comprehensive data platform for collecting, processing, and analyzing renewable energy generation data. The platform integrates real-time telemetry from **Solar**, **Wind**, and **Weather IoT** sources to monitor energy generation across multiple sites and enable intelligent analytics, forecasting, and alerting.

The platform is built on **Databricks** and **Azure**, leveraging the **Medallion Architecture** for scalable, reliable data processing.

---

## Architecture & Technology Stack

- **Cloud Platform:** Microsoft Azure
- **Data Lakehouse:** Databricks with Unity Catalog
- **Data Storage:** Azure Data Lake Storage (ADLS)
- **Real-time Ingestion:** Azure Event Hubs
- **Database:** SQL Server (for asset metadata)
- **Processing:** PySpark with Databricks Notebooks

---

## Medallion Architecture: Three-Layer Data Pipeline

HELIOS uses the **Medallion Architecture** (Bronze → Silver → Gold) to progressively refine and enrich raw data:

### 🔴 **Bronze Layer (Raw Data)**
**Purpose:** Capture raw, unmodified data from source systems  
**Data Sources:**
- Azure Event Hubs ingestion from Solar, Wind, and Weather IoT devices
- Real-time streaming using Spark Structured Streaming with Auto Loader
- AVRO format data from Event Hub endpoints

**Key Notebooks:**
- `Helios_Solar_Bronze.py` - Raw solar telemetry ingestion
- `Helios_Wind_Bronze.py` - Raw wind turbine telemetry ingestion
- `Helios_Weather_Bronze.py` - Raw weather IoT sensor data ingestion

**Stored Location:** `abfss://helios-raw-storage@projectstorage12.dfs.core.windows.net/helios_medallion/bronze/`

**Data Fields (Example - Wind):**
- `asset_id`, `iot_id`, `site_id`, `company_name`
- `windmill_speed`, `power_generated`, `vibration_intensity`, `heat_generated`
- `event_timestamp`, `day_night`, `EnqueuedTimeUtc`

---

### 🟡 **Silver Layer (Cleaned & Deduplicated)**
**Purpose:** Transform and clean raw data for analytical use  
**Processing:**
- Remove duplicates based on device ID and timestamp
- Data quality validation and schema enforcement
- Incremental processing (only new records since last run)
- Add processing timestamps for audit trails

**Key Notebooks:**
- `Helios_Solar_Silver.py` - Cleaned solar data
- `Helios_Wind_Silver.py` - Cleaned wind data
- `Helios_Weather_Silver.py` - Cleaned weather data
- `Validations.py` - Data quality checks

**Stored Location:** `abfss://helios-raw-storage@projectstorage12.dfs.core.windows.net/helios_medallion/silver/`

**Transformations Applied:**
- Deduplication on `(gen_id, event_timestamp)`
- Schema validation and type casting
- Timestamp normalization
- Null value handling

---

### 🟢 **Gold Layer (Business Analytics)**
**Purpose:** Create optimized, aggregated data for reporting and analytics  
**Features:**
- Aggregations by site, device, and time periods
- Dimensional tables (Alert Rules, Device Dimensions)
- Integration with SQL Server asset metadata
- Streaming aggregations for real-time dashboards

**Key Notebooks:**
- `Sql_Server_to_gold.py` - Load asset metadata (Sites, Solar/Wind Assets, IoT Devices)
- `gold_streaming.py` - Real-time streaming aggregations
- `final-aggregations.py` - Time-based energy aggregations
- `finalized-aggreations-devices-and-energy.py` - Device-level analytics
- `Alert Rules Dim Table.py` - Alert configuration management

**Stored Location:** `abfss://helios-raw-storage@projectstorage12.dfs.core.windows.net/helios_medallion/gold/`

**Output Datasets:**
- Hourly/Daily energy generation summaries
- Device performance metrics
- Site-level aggregations
- Alert rules and thresholds

---

## Data Flow & Sources

### 📊 **Data Generators (Simulators)**
Real-time data generation for development and testing:

**Solar Generation:**
- `generator_solar_ts.py` - Tamil Nadu (TN) solar sites
- `generator_solar_tn.py` - Tamil Nadu sites
- `generator_solar_ks.py` - Karnataka (KS) sites
- `generator_solar_kl.py` - Kerala (KL) sites
- `generator_solar_ap.py` - Andhra Pradesh (AP) sites

**Wind Generation:**
- `wind-normal-simulator.py` - Wind turbine telemetry simulation

**Weather Generation:**
- `weather-simulation.py` - Weather IoT sensor data simulation

### 📡 **Event Hub Topics**
- `helios_solar` - Solar panel telemetry
- `helios_wind` - Wind turbine telemetry
- `helios_weather_iot` - Weather sensor data

### 💾 **SQL Server Asset Metadata**
Connection to SQL Server stores reference data:
- `helios_assets.sites` - Site information
- `helios_assets.solar_assets` - Solar panel registrations
- `helios_assets.wind_assets` - Wind turbine registrations
- `helios_assets.iot_devices` - IoT sensor configurations

---

## Medallion Architecture Benefits in HELIOS

| Layer | Benefit |
|-------|---------|
| **Bronze** | Immutable raw data archive; fast ingestion; schema discovery |
| **Silver** | Eliminates duplicates; enforces data quality; enables efficient joins |
| **Gold** | Optimized for analytics; pre-aggregated; business-ready formats |

### Why Medallion Works for Energy Data:
1. **Reliability:** Each layer acts as a checkpoint; raw data is never lost
2. **Scalability:** Incremental processing reduces compute for large volumes
3. **Data Quality:** Validation happens at each layer; issues are caught early
4. **Traceability:** Full audit trail from raw to final datasets
5. **Flexibility:** Different teams can use the layer that suits their needs

---

## Key Use Cases

- **Real-time Energy Monitoring:** Track power generation across all sites
- **Predictive Analytics:** Weather-based solar and wind forecasting
- **Anomaly Detection:** Identify equipment failures via vibration/heat
- **Performance Optimization:** Site and device-level efficiency analysis
- **Alert Management:** Threshold-based alerts for abnormal conditions
- **Regulatory Compliance:** Audit trails and data governance

---

## Project Structure

```
HELIOS/
├── Helios-Databricks-Workspace/
│   ├── Bronze/              # Raw data ingestion notebooks
│   ├── Silver/              # Data cleaning & deduplication
│   ├── Gold/                # Analytics & aggregations
│   ├── Validation/          # Data quality checks
│   └── project_setup.py     # Schema & volume initialization
├── generation_solar/        # Solar data simulators
├── generation_wind/         # Wind data simulators
├── generation_weather_iot/  # Weather data simulators
└── .env                     # Event Hub connection strings
```

---

## Getting Started

1. **Setup:** Run `project_setup.py` in Databricks to create schemas and volumes
2. **Start Data Generators:** Run simulator scripts to produce test data
3. **Execute Bronze Layer:** Trigger Bronze notebooks to ingest data
4. **Execute Silver Layer:** Run Silver notebooks for data cleaning
5. **Execute Gold Layer:** Generate analytics-ready datasets
6. **Validate:** Run Validations notebook to ensure data quality

---

## Configuration & Secrets

Environment variables (stored in `.env`):
- `EVENT_HUB_CONNECTION_STRING_SOLAR` - Solar Event Hub connection
- `EVENT_HUB_CONNECTION_STRING_WIND` - Wind Event Hub connection
- `EVENT_HUB_CONNECTION_STRING_WEATHER` - Weather Event Hub connection

---

## Summary

HELIOS is a production-grade renewable energy intelligence platform that demonstrates best practices in cloud data architecture. The **Medallion Architecture** ensures data moves from raw (Bronze) → clean (Silver) → analytics-ready (Gold), enabling reliable, scalable insights across solar, wind, and weather data sources.

