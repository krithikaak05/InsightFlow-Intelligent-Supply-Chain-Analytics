# 🚚 InsightFlow: Intelligent Supply Chain Analytics

*Cloud-Based ETL Pipeline for Supply Chain Performance Insights*

[![AWS](https://img.shields.io/badge/AWS-Glue%20%7C%20S3%20%7C%20Athena-FF9900?style=flat&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/)
[![PySpark](https://img.shields.io/badge/PySpark-3.4-E25A1C?style=flat&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Tableau](https://img.shields.io/badge/Tableau-Desktop-E97627?style=flat&logo=tableau&logoColor=white)](https://www.tableau.com/)
[![ETL](https://img.shields.io/badge/ETL-End--to--End-00C853?style=flat)](https://github.com)

---

## 📑 Table of Contents

- [Overview](#-overview)
- [Business Objectives](#-business-objectives)
- [Architecture](#️-architecture)
- [Data Model](#-data-model)
- [ETL Pipeline Workflow](#️-etl-pipeline-workflow)
- [Key Results](#-key-results)
- [Security & Governance](#-security--governance)
- [Repository Files](#-repository-files)
- [Future Enhancements](#-future-enhancements)
- [References](#-references)

---

## 📘 Overview

**InsightFlow** is an end-to-end serverless ETL pipeline built on AWS that processes **185K+ supply chain transactions** to deliver real-time insights into delivery performance, operational efficiency, and profitability.

**Key Highlights:**
- 🔄 Automated ETL Pipeline using AWS Glue and PySpark
- 📊 Star Schema Data Warehouse with fact and dimension tables
- ⚡ Serverless Architecture leveraging AWS managed services
- 🎯 Business Intelligence with SQL-based analytics via Athena
- 🔐 Enterprise Security with IAM and CloudWatch monitoring

---

## 🎯 Business Objectives

- **Improve Delivery Performance**: Identify late delivery patterns and optimize shipping modes
- **Customer Segmentation**: Analyze purchasing behavior across customer segments
- **Profit Optimization**: Track profit margins by product category and order size
- **Operational Efficiency**: Monitor shipping days and fulfillment metrics
- **Data-Driven Decisions**: Enable stakeholders to query real-time analytics

---

## 🏗️ Architecture

### System Design

<img width="732" height="369" alt="Screenshot 2025-11-13 at 5 00 16 PM" src="https://github.com/user-attachments/assets/d9c9cc89-7b5e-4ce8-ab32-32d77f66fbc2" />


### Architecture Components

| Layer | AWS Service | Purpose |
|-------|-------------|---------|
| **Storage** | Amazon S3 | Scalable data lake for raw and processed datasets |
| **Metadata Management** | AWS Glue Data Catalog | Centralized schema registry and table definitions |
| **Schema Discovery** | AWS Glue Crawler | Automatic schema detection and metadata extraction |
| **Transformation** | AWS Glue (PySpark) | Distributed data cleaning, deduplication, and aggregation |
| **Orchestration** | AWS Step Functions | Workflow automation and job dependency management |
| **Analytics** | Amazon Athena | Serverless SQL query engine for ad-hoc analysis |
| **Visualization** | Tableau Desktop | Interactive dashboards and business intelligence reports |
| **Security** | IAM + CloudWatch | Access control, logging, and pipeline monitoring |

---

## 📂 Data Model

### Source Files

| File | Records | Description | Key Columns |
|------|---------|-------------|-------------|
| `Customer.csv` | ~50K | Customer master data | `customer_id`, `customer_name`, `segment`, `city`, `state` |
| `Product.csv` | ~5K | Product catalog | `product_id`, `product_name`, `category_id`, `category_name` |
| `Order.csv` | ~65K | Transactional orders | `order_id`, `customer_id`, `product_id`, `quantity`, `profit_ratio` |
| `Delivery.csv` | ~65K | Shipping & logistics | `order_id`, `shipping_days`, `delivery_status`, `shipping_mode` |

### Star Schema Design

**Dimension Tables:**
- `customer_dim` - Customer attributes (name, segment, location)
- `product_dim` - Product metadata (name, category, price)
- `order_dim` - Order details (quantity, discounts, payment type)
- `delivery_dim` - Delivery metrics (status, mode, shipping days)

**Fact Table:**
- `fact_sales` - Aggregated sales metrics (total sales, profit ratios, delivery performance)

---

## ⚙️ ETL Pipeline Workflow

### 1. Data Ingestion
- Upload CSV files to S3 raw zone
- AWS Glue Crawler scans and catalogs schemas
- Metadata registered in Glue Data Catalog

### 2. Dimension Table Creation (ETL Job 1)
**PySpark Transformations:**
- Remove duplicates with `SELECT DISTINCT`
- Handle NULL values with `COALESCE()`
- Standardize IDs with `LPAD()` padding
- Data type casting and validation

**Output:** 4 dimension tables stored as Parquet files with Snappy compression

### 3. Fact Table Aggregation (ETL Job 2)
**Business Logic:**
- Join all dimension tables
- Calculate aggregated metrics (total sales, profit, discounts)
- Create surrogate keys with `ROW_NUMBER()`
- Derive order size categories (Small/Medium/Large)

**Output:** `fact_sales` table with comprehensive business metrics

### 4. Orchestration

<img width="358" height="487" alt="Screenshot 2025-11-13 at 5 00 06 PM" src="https://github.com/user-attachments/assets/5da2772c-7321-406b-a05e-2227490d2ff7" />


**AWS Step Functions** orchestrates the entire pipeline as a state machine, ensuring each component executes in the correct sequence with built-in error handling.

**Workflow Steps:**
1. **Start Crawler**: Scans S3 raw data and discovers table schemas
2. **Wait & Check Status**: Polls every 60 seconds until crawler completes
3. **Start ETL Job 1**: Triggers dimension table creation with PySpark transformations
4. **Wait & Check Status**: Monitors job progress until dimensions are ready
5. **Start ETL Job 2**: Joins dimensions to build the fact table with aggregated metrics
6. **Wait & Check Status**: Ensures fact table creation completes successfully
7. **Workflow Complete**: Pipeline ready for analytics queries

**Key Features:**
- **Sequential Dependencies**: Ensures crawler finishes before ETL jobs start, dimensions ready before fact table
- **Automated Error Handling**: Retries transient failures, alerts on persistent errors
- **Real-Time Monitoring**: CloudWatch logs track execution times and success/failure rates
- **Conditional Logic**: Checks completion status at each stage before proceeding

### 5. Analytics
- Parquet files stored in S3 processed zone
- Glue Catalog updated with new schemas
- Amazon Athena enables SQL-based analytics
- **Tableau Desktop** connects to Athena for interactive dashboard visualizations

---

## 📊 Key Results

### Dashboard Overview

<img width="641" height="494" alt="Screenshot 2025-11-13 at 5 17 52 PM" src="https://github.com/user-attachments/assets/ebb75f0c-bae6-4b8b-abba-98820dde0899" />

*Interactive Tableau dashboard showing key supply chain metrics and performance indicators*

### Pipeline Performance
- ✅ **100% data ingestion** success across all 4 source files
- ⚡ **~8 minutes** total pipeline runtime
- 📦 **70% compression** ratio with Parquet format
- 🎯 **<5 second** query response times in Athena
- 🔍 **0 duplicates**, <0.1% null values after cleaning

### Business Metrics

| KPI | Value | Insight |
|-----|-------|---------|
| **Products Ordered** | 334,107 | Total units processed |
| **Gross Sales** | $32,314,681.23 | Revenue generated |
| **Net Profit** | $377,390.65 | Bottom-line profitability |
| **Orders Placed** | 65,752 | Transaction volume |
| **Avg Shipping Days** | ~4 days | Fulfillment speed |
| **Late Deliveries** | 33,423 | Orders delayed across all modes |

### Key Findings (via Tableau Dashboard)
- 🏆 **Consumer segment** contributes 51% of net profit
- 🚚 **Standard class** shipping has highest late delivery rate (~55%)
- 📉 **Fishing category** shows lowest profit margin (0.142 vs avg 2.282)
- ⚡ **Top 5 products** drive significant revenue:
  - Fire Safe: $3.5M+
  - Exercise Equipment: $3.2M+
  - Bike: $2.8M+
  - Kayak: $2.5M+
  - Football Shoes: $2.3M+
- 📦 **On-time delivery** varies by mode: First Class (48%), Same Day (52%), Second Class (50%), Standard (55%)

**Dashboard Features:**
- Interactive filters for Quarter, Year, Shipping Mode, Product Category
- Drill-down capabilities from customer to product level
- Real-time visualizations: profit margins, delivery performance, sales trends
- Treemap for profit margin analysis across categories
- Time-series analysis of shipping days across months

---

## 🔐 Security & Governance

**IAM Security:**
- Least-privilege access policies for Glue, S3, and Athena
- Service roles with restricted permissions
- VPC endpoints for private connectivity

**Monitoring:**
- CloudWatch logs for all ETL jobs and workflows
- Custom metrics and alarms for pipeline failures
- Cost allocation tags for budget tracking

**Compliance:**
- S3-SSE encryption at rest
- TLS 1.2+ encryption in transit
- Audit trails via CloudWatch and Athena query history

---

## 📂 Repository Files

```
├── AWS_STEP_FUNCTION_ORCHESTRATION.png
├── AWS_SYSTEM_DESIGN.png
├── Data.zip
├── ETL_JOB1.py
├── ETL_JOB2.py
├── TABLEAU_DASHBOARD.png
├── TransformationsQueries_ETL.sql
└── readme.md
```
- **AWS_STEP_FUNCTION_ORCHESTRATION.png** — Step Function orchestration diagram  
- **AWS_SYSTEM_DESIGN.png** — System design architecture  
- **Data.zip** — Dataset used for pipeline  
- **ETL_JOB1.py** — Python ETL Job 1  
- **ETL_JOB2.py** — Python ETL Job 2  
- **TABLEAU_DASHBOARD.png** — Tableau dashboard output  
- **TransformationsQueries_ETL.sql** — SQL transformation logic  
- **readme.md** — Documentation  

---

## 🔮 Future Enhancements

**Phase 2 Roadmap:**
- [ ] Amazon Redshift integration for faster analytics (10-100x speedup)
- [ ] AWS QuickSight interactive dashboards
- [ ] Real-time streaming with Lambda + Kinesis
- [ ] SageMaker ML models for late delivery prediction
- [ ] dbt integration for SQL-based transformations

**Advanced Analytics:**
- [ ] Customer lifetime value (CLV) modeling
- [ ] Inventory optimization with demand forecasting
- [ ] Route optimization using geospatial analysis
- [ ] Anomaly detection for fraud prevention

---

## 📚 References

- **Dataset:** [DataCo SMART SUPPLY CHAIN FOR BIG DATA ANALYSIS](https://data.mendeley.com/datasets/8gx2fvg2k6/5)
- **Attribution:** Fabian Constante, Fernando Silva, Antônio Pereira - Instituto Politécnico de Leiria

---

<div align="center">

**Built with ☁️ AWS | Powered by 🐍 PySpark**

*For detailed technical documentation, see project files*

</div>
