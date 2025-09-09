# Inventory Optimization Platform

**Overview:**  
This is an end-to-end platform for **sales forecasting and inventory optimization**. It ingests sales and inventory data, processes it through an ETL pipeline, generates forecasts and reorder recommendations, and visualizes insights on a dashboard.

---

## Table of Contents
- [Project Structure](#project-structure)
- [Architecture](#architecture)
- [Infrastructure Setup](#infrastructure-setup)
- [Data Ingestion](#data-ingestion)
- [ETL Pipeline](#etl-pipeline)
- [Modeling](#modeling)
- [Serving Layer](#serving-layer)
- [Visualization](#visualization)
- [Technologies Used](#technologies-used)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

---

## Project Structure

The project consists of six main layers:

1. **Infrastructure**: Docker Compose setup with Kafka, Zookeeper, MinIO, PostgreSQL, and optional MLflow & Grafana.  
2. **Ingestion Layer**: Producers generate simulated sales and inventory events → Kafka → Consumer writes JSON batches into MinIO raw zone.  
3. **ETL Layer**: Raw JSON → cleaned Parquet → staged → loaded into PostgreSQL warehouse. Automated via Airflow DAGs.  
4. **Modeling Layer**: Forecast sales using Prophet/Darts/XGBoost. Inventory optimization includes safety stock and reorder points. Metrics and models tracked in MLflow.  
5. **Serving Layer**: FastAPI endpoints for forecasts and reorder recommendations.  
6. **Visualization Layer**: Streamlit dashboard for historical sales, forecasts, stock levels, and reorder recommendations. Optional Apache Superset dashboards for BI-style analytics.

---

## Architecture

```text
+------------------+       +-----------------+       +----------------+
|  Data Producers  |  -->  |      Kafka      |  -->  |   MinIO Raw    |
| (Simulated Sales |       |   & Zookeeper   |       |    Storage     |
|  & Inventory)    |       +-----------------+       +----------------+
+------------------+                                      |
                                                          v
                                                     +----------+
                                                     |  ETL     |
                                                     | (Airflow)|
                                                     +----------+
                                                          |
                                                          v
                                                     +----------+
                                                     |Warehouse |
                                                     |PostgreSQL|
                                                     +----------+
                                                          |
                                                          v
                                                  +---------------+
                                                  | Modeling Layer|
                                                  | (Prophet/XGB)|
                                                  +---------------+
                                                          |
                                                          v
                                                  +---------------+
                                                  | Serving Layer |
                                                  |   (FastAPI)   |
                                                  +---------------+
                                                          |
                                                          v
                                                  +---------------+
                                                  | Visualization |
                                                  |  (Streamlit)  |
                                                  +---------------+
```

## Infrastructure Setup

Use Docker Compose to spin up the necessary services:

```bash
docker-compose up -d
```

**Services included:**
- Kafka + Zookeeper
- MinIO (Data Lake)
- PostgreSQL (Data Warehouse)
- Optional: MLflow, Grafana

Verify that all services are running:

```bash
docker ps
```
## Data Ingestion

**Producer:**
Simulates sales and inventory events and sends them to Kafka.

**Consumer:**
Subscribes to Kafka topics and writes batches into MinIO raw zone in JSON format.

Bucket structure example in MinIO:
```bash
minio-bucket/
└── raw/
    ├── sales/
    └── inventory/
```

## ETL Pipeline

**Batch Processing:**
- Converts raw JSON files → cleaned Parquet format
- Cleans and validates data
- Loads into staging and warehouse tables in PostgreSQL

**Automation:**
- Airflow DAG schedules and orchestrates the pipeline:

```bash
raw → staging → PostgreSQL warehouse
```

## Modeling Layer

**Sales Forecasting**: Prophet, Darts, or XGBoost models

**Inventory Optimization**: Compute safety stock, reorder point, and recommended order quantity

**Tracking**: Models and metrics stored in MLflow

## Visualization Layer

**Streamlit Dashboard**:

- Interactive Plotly charts for historical sales and forecasts
- Current stock levels from warehouse
- Recommended reorder points