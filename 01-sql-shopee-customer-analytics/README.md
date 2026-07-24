# E-Commerce Data Analytics Portfolio

## 📌 Project Overview
This repository contains a collection of advanced SQL queries designed to solve real-world e-commerce business problems. The dataset used in this project is sourced from a real Shopee seller's store, providing hands-on experience with messy, real-world data.

The overarching goal of this project is to extract actionable insights to drive business growth, optimize marketing spend, and improve customer retention.

## 🛠️ Tech Stack & Tools
- **Database:** PostgreSQL
- **Data Engineering:** Python, Pandas (Custom ETL pipeline built to cleanse and merge messy `.csv` and `.xlsx` reports from Seller Centre)
- **Language:** SQL (Window Functions, CTEs, Data Type Casting, Aggregate Functions)
- **Visualization:** *(Upcoming - Power BI Dashboard)*

---

## 📂 Business Questions & SQL Solutions

This portfolio addresses three critical business domains using SQL:

### 1. Customer Segmentation (RFM Analysis)
**File:** [`01_customer_segmentation_rfm.sql`](01_customer_segmentation_rfm.sql)
- **Objective:** Segment customers based on their purchasing behavior using the Recency, Frequency, Monetary (RFM) model.
- **SQL Concepts Used:** `NTILE()`, `EXTRACT()`, CTEs, `CASE WHEN`
- **Business Impact:** Identified "VIP Customers" for targeted loyalty programs and "Churning Customers" for re-engagement campaigns.

### 2. Sales Growth & Running Total Analysis
**File:** [`02_sales_growth_analysis.sql`](02_sales_growth_analysis.sql)
- **Objective:** Track Month-over-Month (MoM) revenue growth and calculate cumulative sales over time.
- **SQL Concepts Used:** `LAG()`, Window Functions (`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`), `DATE_TRUNC()`.
- **Business Impact:** Enabled the management team to monitor sales momentum and ensure the business is meeting its quarterly targets.

### 3. Marketing ROI & Ad Spend Optimization
**File:** [`03_marketing_roi_optimization.sql`](03_marketing_roi_optimization.sql)
- **Objective:** Evaluate the performance of Shopee Ad campaigns and identify which campaigns are profitable vs. which are bleeding money.
- **SQL Concepts Used:** `COALESCE()`, `NULLIF()`, Division by zero handling, Aggregation, Conditional Logic.
- **Key Insight:** Calculated the true **Break-even ROAS (2.22x)** based on a 45% net profit margin (after deducting ~25% platform fees). Any campaign yielding a ROAS below 2.22 is flagged as a loss, empowering the marketing team to immediately cut inefficient ad spend.

---

## 💡 Key Learnings
- **Data Cleaning is Crucial:** Real-world e-commerce data often has changing column names, missing values, and misaligned data types. Building a robust ETL pipeline in Python prior to SQL analysis was essential.
- **Business Logic > SQL Syntax:** Writing the query is only half the battle. Understanding *why* a ROAS of 2.0 is actually a loss (due to platform fees) is what separates a good query from a great business decision.

---
*Created by Sirawit*
