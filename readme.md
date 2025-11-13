# 🚚 InsightFlow: Intelligent Supply Chain Analytics
*A Cloud-Based Data Pipeline for Predictive Supply Chain Insights*

![AWS](https://img.shields.io/badge/AWS-Glue%20%7C%20S3%20%7C%20Athena-orange)
![PySpark](https://img.shields.io/badge/PySpark-Data%20Transformation-blue)
![ETL](https://img.shields.io/badge/ETL-End%20to%20End-green)

---

## 📘 Project Overview
**InsightFlow** is an intelligent supply chain analytics pipeline developed using AWS services.  
It automates the ingestion, transformation, and analysis of **DataCo Global’s Supply Chain dataset**, improving delivery performance, operational efficiency, and profitability through real-time analytics.

This project implements an **end-to-end cloud-based ETL pipeline** integrating structured and unstructured data into a single, scalable AWS environment.

---

## 🎯 Objectives
- Ingest raw structured data from **Amazon S3**
- Transform data into clean, analytics-ready **dimension** and **fact tables**
- Automate orchestration using **AWS Glue** and **Step Functions**
- Enable query and visualization through **Amazon Athena**
- Extract business insights into **delivery risk, customer segmentation, and profit analysis**

---

## 🏗️ Architecture Diagram

### **Architecture Overview**
The solution follows a modular, serverless design across five main layers:
1. **Storage (S3)** – Centralized data lake for raw and processed files  
2. **Schema Detection (Glue Crawler)** – Automatically identifies structure and registers metadata  
3. **Transformation (AWS Glue ETL Jobs)** – Cleans, standardizes, and aggregates data  
4. **Orchestration (AWS Step Functions)** – Manages job execution order  
5. **Analytics (Athena)** – Provides SQL-based validation and reporting  

### **Architecture Flow (Mermaid Diagram)**

```mermaid
flowchart TD
    A[Raw Data - CSV Files] -->|Upload| B[S3 Data Lake]
    B -->|Crawl Schema| C[AWS Glue Crawler]
    C -->|Register Tables| D[AWS Glue Data Catalog]
    D -->|Run PySpark ETL| E[AWS Glue Job 1 - Dimensions]
    E -->|Generate| F[customer_dim, product_dim, order_dim, delivery_dim]
    F -->|Join & Aggregate| G[AWS Glue Job 2 - fact_sales]
    G -->|Output Parquet Files| H[S3 Processed Data]
    H -->|Register Schema| I[Glue Data Catalog (Processed)]
    I -->|Query| J[Amazon Athena]
    J -->|Visualization| K[InsightFlow Dashboard]
    K -->|KPIs & Trends| L[Business Insights]
