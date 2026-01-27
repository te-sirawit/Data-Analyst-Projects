-- [PREPARATION]
-- Import CSV Shopee data into SQL database
-- Configure date style to ISO (Day-Month-Year) to match CSV format in Shopee data
SET datestyle = 'ISO, DMY';
-- Creating master table for clean data
DROP TABLE IF EXISTS inthome_sales;
CREATE TABLE inthome_sales (
	order_date DATE,
	total_revenue NUMERIC(10,2),
    total_orders INTEGER,
    avg_order_value NUMERIC(10,2),
    page_views INTEGER,
    visitors INTEGER
);

-- [STAGING AREA]
-- Create temporary staging table using text data type to prepare for ETL process 
DROP TABLE IF EXISTS inthome_sales_staging;
CREATE TABLE inthome_sales_staging (
    order_date TEXT,
    total_revenue TEXT,
    total_orders TEXT,
    avg_order_value TEXT,
    page_views TEXT,
    visitors TEXT
);

-- ** ETL Process (Extract / Transform / Load) **
-- [EXTRACT]
-- Import CSV file from source to the database in staging area
-- Clear staging area
TRUNCATE TABLE inthome_sales_staging;
-- Import sales data : October 2021
COPY inthome_sales_staging
FROM './dataset/int.hometh.shopee-shop-stats.20211001-20211031.csv'
DELIMITER ','
CSV HEADER;


-- [TRANSFORM & LOAD]
-- Clean the data (remove commas in data) and insert it to the master table (inthome_sales)
INSERT INTO inthome_sales (
    order_date,
    total_revenue,
    total_orders,
    avg_order_value,
    page_views,
    visitors
)
SELECT 
    TO_DATE(order_date, 'DD-MM-YYYY'),
    REPLACE(total_revenue, ',', '')::NUMERIC(10,2),
    REPLACE(total_orders, ',', '')::INTEGER,
    REPLACE(avg_order_value, ',', '')::NUMERIC(10,2),
    REPLACE(page_views, ',', '')::INTEGER,
    REPLACE(visitors, ',', '')::INTEGER
FROM inthome_sales_staging;


-- [IMPORT REMAINING DATA]
-- Repeat the ETL process for the remaining month
-- Import sales data : November 2021
TRUNCATE TABLE inthome_sales_staging;

COPY inthome_sales_staging
FROM './dataset/int.hometh.shopee-shop-stats.20211101-20211130.csv'
DELIMITER ','
CSV HEADER;

INSERT INTO inthome_sales (
    order_date,
    total_revenue,
    total_orders,
    avg_order_value,
    page_views,
    visitors
)
SELECT 
    TO_DATE(order_date, 'DD-MM-YYYY'),
    REPLACE(total_revenue, ',', '')::NUMERIC(10,2),
    REPLACE(total_orders, ',', '')::INTEGER,
    REPLACE(avg_order_value, ',', '')::NUMERIC(10,2),
    REPLACE(page_views, ',', '')::INTEGER,
    REPLACE(visitors, ',', '')::INTEGER
FROM inthome_sales_staging;


-- Import sales data : December 2021
TRUNCATE TABLE inthome_sales_staging;

COPY inthome_sales_staging
FROM './dataset/int.hometh.shopee-shop-stats.20211201-20211231.csv'
DELIMITER ','
CSV HEADER;

INSERT INTO inthome_sales (
    order_date,
    total_revenue,
    total_orders,
    avg_order_value,
    page_views,
    visitors
)
SELECT 
    TO_DATE(order_date, 'DD-MM-YYYY'),
    REPLACE(total_revenue, ',', '')::NUMERIC(10,2),
    REPLACE(total_orders, ',', '')::INTEGER,
    REPLACE(avg_order_value, ',', '')::NUMERIC(10,2),
    REPLACE(page_views, ',', '')::INTEGER,
    REPLACE(visitors, ',', '')::INTEGER
FROM inthome_sales_staging;


-- Import sales data : January 2022
TRUNCATE TABLE inthome_sales_staging;

COPY inthome_sales_staging
FROM './dataset/int.hometh.shopee-shop-stats.20220101-20220131.csv'
DELIMITER ','
CSV HEADER;

INSERT INTO inthome_sales (
    order_date,
    total_revenue,
    total_orders,
    avg_order_value,
    page_views,
    visitors
)
SELECT 
    TO_DATE(order_date, 'DD-MM-YYYY'),
    REPLACE(total_revenue, ',', '')::NUMERIC(10,2),
    REPLACE(total_orders, ',', '')::INTEGER,
    REPLACE(avg_order_value, ',', '')::NUMERIC(10,2),
    REPLACE(page_views, ',', '')::INTEGER,
    REPLACE(visitors, ',', '')::INTEGER
FROM inthome_sales_staging;


-- Import sales data : February 2022
TRUNCATE TABLE inthome_sales_staging;

COPY inthome_sales_staging
FROM './dataset/int.hometh.shopee-shop-stats.20220201-20220228.csv'
DELIMITER ','
CSV HEADER;

INSERT INTO inthome_sales (
    order_date,
    total_revenue,
    total_orders,
    avg_order_value,
    page_views,
    visitors
)
SELECT 
    TO_DATE(order_date, 'DD-MM-YYYY'),
    REPLACE(total_revenue, ',', '')::NUMERIC(10,2),
    REPLACE(total_orders, ',', '')::INTEGER,
    REPLACE(avg_order_value, ',', '')::NUMERIC(10,2),
    REPLACE(page_views, ',', '')::INTEGER,
    REPLACE(visitors, ',', '')::INTEGER
FROM inthome_sales_staging;


-- Import sales data : March 2022
TRUNCATE TABLE inthome_sales_staging;

COPY inthome_sales_staging
FROM './dataset/int.hometh.shopee-shop-stats.20220301-20220331.csv'
DELIMITER ','
CSV HEADER;

INSERT INTO inthome_sales (
    order_date,
    total_revenue,
    total_orders,
    avg_order_value,
    page_views,
    visitors
)
SELECT 
    TO_DATE(order_date, 'DD-MM-YYYY'),
    REPLACE(total_revenue, ',', '')::NUMERIC(10,2),
    REPLACE(total_orders, ',', '')::INTEGER,
    REPLACE(avg_order_value, ',', '')::NUMERIC(10,2),
    REPLACE(page_views, ',', '')::INTEGER,
    REPLACE(visitors, ',', '')::INTEGER
FROM inthome_sales_staging;


-- Checking all imported data
SELECT * FROM inthome_sales ORDER BY order_date;
SELECT *
FROM inthome_sales;

-- [Data Analysis]
-- Monthly Sales Performance Breakdown (Q4 2021)
SELECT 
	TO_CHAR(order_date, 'Month') AS month, 
	SUM(total_revenue) AS total_revenue,
	SUM(total_orders) AS total_orders
FROM inthome_sales
WHERE EXTRACT(QUARTER FROM order_date) = 4 AND EXTRACT(YEAR FROM order_date) = 2021
GROUP BY 1, EXTRACT(MONTH FROM order_date)
ORDER BY EXTRACT(MONTH FROM order_date);

-- Seasonality Analysis: Average Sales by Day of the Week
-- Comparing Peak Season (Q4 2021) vs Post-Season (Q1 2022)
-- Analysing customer shopping routine
SELECT
	CASE
		WHEN order_date BETWEEN '2021-10-01' AND '2021-12-31' THEN 'Q4_2021'
		WHEN order_date BETWEEN '2022-01-01' AND '2022-03-31' THEN 'Q1_2022'
	END AS period,
	TO_CHAR(order_date, 'FMDay') AS day_name,
	ROUND(AVG(total_revenue),2) AS avg_daily_sales
FROM inthome_sales
WHERE 
	order_date BETWEEN '2021-10-01' AND '2022-03-31'
GROUP BY 1, 2, EXTRACT(ISODOW FROM order_date)
ORDER BY 1,EXTRACT(ISODOW FROM order_date);


-- Quarter-over-Quarter (QoQ) Growth Rate
-- Using CTE and Window Functions(LAG) to calculate the QoQ growth rate
WITH quarterly_sales AS (
SELECT
	DATE_TRUNC('quarter', order_date) AS quarter_start,
	SUM(total_revenue) AS current_sales
FROM inthome_sales
GROUP BY 1
)
SELECT
	TO_CHAR(quarter_start, 'YYYY-"Q"Q') AS quarter,
	current_sales,
	LAG(current_sales) OVER (ORDER BY quarter_start) AS previous_sales,
	ROUND(
		(current_sales - LAG(current_sales) OVER (ORDER BY quarter_start))* 100/NULLIF(LAG(current_sales) OVER (ORDER BY quarter_start),0) 
	,2) AS "%Growth"
FROM quarterly_sales
;
