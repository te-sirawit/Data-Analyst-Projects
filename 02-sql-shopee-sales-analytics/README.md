# Shopee Sales & Operations Analytics

## 📌 Project Overview
This project focuses on extracting, transforming, and analyzing e-commerce sales data for an active **Shopee Store** from Q4 2021 to Q1 2022. The objective is to monitor sales performance, understand customer shopping behavior, and evaluate conversion efficiency to optimize future business strategies and operational readiness.

## 🛠️ Tech Stack & Skills Demonstrated
- **Database:** PostgreSQL
- **Data Engineering (ETL):** Built an SQL-based ETL pipeline using `COPY`, Data Type Casting, and String Manipulation.
- **Data Analysis:** Window Functions (`LAG`, `AVG OVER`), `DATE_TRUNC`, CTEs, Conditional Logic (`CASE WHEN`).

---

## ⚙️ ETL Pipeline (Extract, Transform, Load)

Real-world data is rarely clean. This project demonstrates the ability to ingest raw `.csv` files into a database and transform them into an analysis-ready state entirely using SQL.

1. **Extract:** Created a `staging` table with `TEXT` data types and used the `COPY` command to bulk-insert raw CSV files month-by-month.
2. **Transform:** 
   - Cleansed numerical data by removing thousands separators (`REPLACE(..., ',', '')`).
   - Standardized date formats using `TO_DATE()`.
   - Casted raw strings into appropriate `NUMERIC` and `INTEGER` data types.
3. **Load:** Inserted the clean, transformed data into the master `shopee_sales` table.

---

## 📊 Key Analytical Queries

Once the data was structured, several advanced SQL queries were executed to extract business insights:

### 1. Seasonality Analysis (Peak vs. Post-Season)
- Analyzed the average daily sales categorized by the **Day of the Week**.
- Compared customer shopping routines between the Peak Season (Q4 2021) and Post-Season (Q1 2022) to optimize operational readiness and ad-bidding times.

### 2. Quarter-over-Quarter (QoQ) Growth
- Used **Common Table Expressions (CTEs)** and the `LAG()` window function to dynamically calculate the QoQ revenue growth rate.
- Included `NULLIF()` division error handling for robust query execution.

### 3. 7-Day Moving Average
- Implemented a rolling 7-day moving average (`ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`) to smooth out daily sales fluctuations and identify the true underlying sales trend.

### 4. Conversion Efficiency (CVR & RPV)
- Evaluated the **Conversion Rate (CVR)** and **Revenue Per Visitor (RPV)** broken down by the day of the week.
- This insight is crucial for determining which days offer the highest return on investment (ROI) for marketing campaigns.

---
*Created by Sirawit*
