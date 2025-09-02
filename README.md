# 📦 Retail Sales Data Engineering Pipeline

An end-to-end data engineering pipeline for processing online retail data, demonstrating modern data architecture patterns with containerized deployment.

## 🏗️ Architecture Overview

This project implements a complete data engineering workflow with the following components:

- **Data Source**: Kaggle API
- **Data Ingestion**: Apache NiFi for automated data collection
- **Storage Layer**: MinIO for raw and processed data (S3-compatible)
- **ETL & Orchestration**: Prefect for workflow automation and data transformation
- **Data Warehouse**: PostgreSQL with dimensional modeling (star schema)
- **Visualization**: Power BI dashboards with real-time analytics
- **Infrastructure**: Docker for containerization and reproducibility

### Pipeline Architecture

![Pipeline Architecture](./docs/pipeline_diagram.png)

## 📁 Project Structure

```
.
├── docker-compose.yaml          # Docker environment configuration
├── Dockerfile                   # Container setup
├── requirements.txt             # Python dependencies
├── README.md                    # Project documentation
├── .gitignore                   # Git ignore rules
│
├── data/                        # Dataset storage
│   └── raw/
│       └── sales.csv           # Sample dataset
│
├── flows/                       # Prefect workflow definitions
│   └── etl_sales_flow.py       # Main ETL pipeline
│
├── nifi/                        # Apache NiFi configurations
│   └── flows/
│       └── http_to_s3.xml      # Data ingestion flow
│
├── PowerBI/                     # Business Intelligence
│   └── Dashboard.pbix          # Power BI dashboard file
│
├── sql_scripts/                 # Database schema and views
│   ├── script1.sql             # Database initialization
│   ├── script2.sql             # Dimension tables
│   ├── script3.sql             # Fact tables
│   ├── script4.sql             # Views creation
│   └── script5.sql             # Indexes and constraints
│
└── docs/                        # Documentation
    └── pipeline_diagram.png    # Architecture diagram
```

## 🔧 Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Orchestration** | Apache Prefect | Workflow automation and scheduling |
| **Data Ingestion** | Apache NiFi | Data collection and routing |
| **Object Storage** | MinIO | S3-compatible storage for raw/processed data |
| **Data Warehouse** | PostgreSQL | Structured data storage with OLAP capabilities |
| **Containerization** | Docker & Docker Compose | Environment consistency and deployment |
| **Visualization** | Microsoft Power BI | Business intelligence and dashboards |
| **Language** | Python | ETL scripts and data processing |

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Power BI Desktop (for dashboard viewing)

### Service Endpoints

| Service | URL |
|---------|-----|
| **MinIO Console** | http://localhost:9001 |
| **NiFi UI** | http://localhost:8080/nifi |
| **Prefect UI** | http://localhost:4200 |
| **PostgreSQL** | localhost:5432 |

## 📊 Data Model

The pipeline implements a **star schema** optimized for analytical queries:

### Fact Table
- **FactSales**: Central fact table containing sales transactions

### Dimension Tables
- **DimDate**: Date dimension with calendar hierarchy
- **DimProduct**: Product catalog and attributes
- **DimCustomer**: Customer demographics and segmentation

### Key Metrics
- Total Revenue
- Units Sold
- Average Order Value
- Customer Lifetime Value
- Product Performance
- Seasonal Trends

## 🔄 Pipeline Workflow

### 1. Data Ingestion
- NiFi monitors Kaggle API for new data
- Raw data stored in MinIO bucket (`raw-data/`)
- Automated file validation and metadata extraction

### 2. ETL Processing
- Prefect orchestrates the transformation pipeline
- Data quality checks and cleansing
- Business logic application
- Dimensional modeling transformation

### 3. Data Loading
- Processed data stored in MinIO (`processed-data/`)
- Index optimization for query performance

### 4. Analytics & Reporting
- PostgreSQL views for simplified BI access
- Power BI connects via import
