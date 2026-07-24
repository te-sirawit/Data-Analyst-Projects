# 📊 Data Analyst Portfolio

Welcome to my Data Analytics Portfolio! This repository contains a collection of business-driven data projects demonstrating my skills in Data Engineering, Data Modeling, and Business Intelligence.

## 📌 Project Directory (Table of Contents)

| Project Folder | Core Tools | Business Highlights |
|---|---|---|
| **1. [Shopee Customer & Marketing Analytics](01-sql-shopee-customer-analytics)** | **SQL** | RFM Customer Segmentation, Marketing ROI Optimization (Break-even ROAS) |
| **2. [Shopee Sales & Operations Analytics](02-sql-shopee-sales-analytics)** | **SQL** | ETL Data Pipeline, Seasonality Analysis, Moving Averages (Window Functions) |
| **3. [Shopee E-Commerce Performance Dashboard](03-powerbi-sql-shopee-dashboard)** | **Power BI**, **SQL**, **Python** | End-to-end Star Schema Data Modeling, Advanced DAX (Dynamic Time Intelligence), Interactive Dashboards |

---

## 📂 Project Summaries

### [01. Shopee Customer & Marketing Analytics](01-sql-shopee-customer-analytics)
*An advanced SQL analytics project focusing on customer behavior and marketing optimization for an active Shopee store.*
- **Tech Stack:** PostgreSQL (Neon Cloud DB)
- **Key Analyses:**
  - Customer Segmentation using the **RFM Model** (Recency, Frequency, Monetary).
  - Tracked **Month-over-Month (MoM)** revenue growth and Running Totals.
  - Optimized marketing spend by calculating the true **Break-even ROAS (2.22x)** after accounting for all platform fees, identifying campaigns that were bleeding money.

### [02. Shopee Sales & Operations Analytics](02-sql-shopee-sales-analytics)
*A robust SQL-based ETL pipeline and performance analysis focusing on store operations, seasonal trends, and conversion metrics.*
- **Tech Stack:** PostgreSQL (Neon Cloud DB)
- **Key Analyses:** 
  - **ETL Pipeline:** Built SQL-based staging tables and used `COPY` for bulk data ingestion and transformation.
  - Analyzed peak vs. post-season **Seasonality** and Day of Week shopping routines.
  - Calculated **Quarter-over-Quarter (QoQ) Growth** and **7-Day Moving Averages** using advanced Window Functions.

### [03. Shopee E-Commerce Performance Dashboard](03-powerbi-sql-shopee-dashboard)
*An end-to-end data analytics pipeline designed to evaluate the sales performance and advertising efficiency of a Shopee storefront via an interactive Power BI Dashboard.*
- **Tech Stack:** Power BI, SQL, Python (AI-Assisted ETL)
- **Key Analyses & Features:**
  - Designed an optimized **Star Schema** to connect Fact tables (Orders, Ads) with Dimension tables (Products, Calendar).
  - Developed Advanced DAX measures for **Dynamic Time Intelligence (`ISINSCOPE`)** to automatically switch between MoM and YoY growth.
  - Tracked Core Advertising KPIs such as **ROAS**, **CTR**, and **CPA**.

---
*Created by Sirawit Techachaikulsiri*
