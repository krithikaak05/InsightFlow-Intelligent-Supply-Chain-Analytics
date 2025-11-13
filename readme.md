# 🚚 InsightFlow: Intelligent Supply Chain Analytics

*Cloud-Based ETL Pipeline for Supply Chain Performance Insights*

[![AWS](https://img.shields.io/badge/AWS-Glue%20%7C%20S3%20%7C%20Athena-FF9900?style=flat&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/)
[![PySpark](https://img.shields.io/badge/PySpark-3.4-E25A1C?style=flat&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![ETL](https://img.shields.io/badge/ETL-End--to--End-00C853?style=flat)](https://github.com)

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

<img width="732" height="369" alt="Screenshot 2025-11-13 at 5 00 16 PM" src="https://github.com/user-attachments/assets/1643785a-e835-41cc-90f5-5ef0efb8a53c" />


### Architecture 


| Layer | AWS Service | Purpose |
|-------|-------------|---------|
| **Storage** | Amazon S3 | Scalable data lake for raw and processed datasets |
| **Metadata Management** | AWS Glue Data Catalog | Centralized schema registry and table definitions |
| **Schema Discovery** | AWS Glue Crawler | Automatic schema detection and metadata extraction |
| **Transformation** | AWS Glue (PySpark) | Distributed data cleaning, deduplication, and aggregation |
| **Orchestration** | AWS Step Functions | Workflow automation and job dependency management |
| **Analytics** | Amazon Athena | Serverless SQL query engine for ad-hoc analysis |
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

<img width="358" height="487" alt="Screenshot 2025-11-13 at 5 00 06 PM" src="https://github.com/user-attachments/assets/dd16dec6-0896-41a5-b358-0ba6ff8a0a50" />

AWS Step Functions manages workflow:
- Crawler execution → ETL Job 1 → ETL Job 2
- Automated error handling and retry logic
- CloudWatch monitoring and alerts

### 5. Analytics
- Parquet files stored in S3 processed zone
- Glue Catalog updated with new schemas
- Amazon Athena enables SQL-based analytics

---

## 📊 Key Results

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
| **Gross Sales** | $32.3M | Revenue generated |
| **Net Profit** | $377K | Bottom-line profitability |
| **Orders Placed** | 65,752 | Transaction volume |
| **Avg Shipping Days** | ~4 days | Fulfillment speed |

### Key Findings
- 🏆 **Consumer segment** contributes 51% of net profit
- 🚚 **Standard class** shipping has highest late delivery rate
- 📉 **Fishing category** shows -12% profit margin (requires review)
- ⚡ **Top 5 products** drive 40% of total revenue
- 📦 **Large orders** (10+ units) account for 28% of revenue

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
- **Date:** December 7, 2024

---

<div align="center">

**Built with ☁️ AWS | Powered by 🐍 PySpark**

*For detailed technical documentation, see project files*

</div>
