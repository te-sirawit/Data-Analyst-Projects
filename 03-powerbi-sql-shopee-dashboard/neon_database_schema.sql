-- Purpose : Provision tables in Neon PostgreSQL for pgAdmin import and Power BI DirectQuery/Import
-- Source: fact_order.csv, fact_ad.csv, dim_product.csv

-- Reset tables
DROP TABLE IF EXISTS fact_order;
DROP TABLE IF EXISTS fact_ad;
DROP TABLE IF EXISTS dim_product;

-- Create fact and dimension tables
CREATE TABLE fact_order (
	order_date DATE,
	order_id VARCHAR(50),
	user_id VARCHAR(100),
	product_name VARCHAR(255),
	province VARCHAR(100),
	shipping_option VARCHAR(255),
	total_amount NUMERIC(10,2),
	order_status VARCHAR(50)
);

CREATE TABLE fact_ad (
	campaign_name VARCHAR(255),
	product_id VARCHAR(50),
	impressions INTEGER,
	clicks INTEGER,
	orders INTEGER,
	ad_spend NUMERIC(10,2),
	ad_revenue NUMERIC(10,2),
	campaign_month TEXT, 
	campaign_year TEXT
);

CREATE TABLE dim_product (
	product_id VARCHAR(50),
	product_name VARCHAR(255),
	category VARCHAR(100)
);

-- Verify data after CSV import
SELECT *
FROM fact_order;

SELECT *
FROM fact_ad;

SELECT *
FROM dim_product;