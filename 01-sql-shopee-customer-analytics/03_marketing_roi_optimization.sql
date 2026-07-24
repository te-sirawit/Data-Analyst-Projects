-- ==========================================
-- E-Commerce Marketing ROI & Ad Performance
-- ==========================================
-- Business Question: 
-- Which ad campaigns are yielding the best Return on Ad Spend (ROAS)?
-- Which campaigns should we stop because they are bleeding money (High CPA/Low ROAS)?


SELECT *
FROM ads_performance_real;

-- 1. Summarize ad performance data
WITH ads_summary AS (
	SELECT
		ad_name,
		item_id,
		ad_type,
		SUM(impressions) AS total_impressions,
		SUM(clicks) AS total_clicks,
		SUM(orders) AS total_orders,
		SUM(expense) AS total_ad_spend,
		SUM(sales) AS total_sales_generated
	FROM 
		ads_performance_real
	WHERE 
		clicks >= 50 -- delete noise from beginning or deleted campaign
	GROUP BY 
		ad_name, item_id, ad_type
),

-- 2. Calculate Click-Through Rate (CTR), Cost Per Acquisition (CPA), and Return on Ad Spend (ROAS)
ads_metrics AS (
	SELECT
		ad_name,
		item_id,
		ad_type,
		total_impressions,
		total_clicks,
		total_orders,
		total_ad_spend,
		total_sales_generated,
		COALESCE(
			ROUND((total_clicks / NULLIF(total_impressions,0) * 100)::NUMERIC, 2)
				, 0) AS CTR,
		COALESCE(
			ROUND((total_ad_spend / NULLIF(total_orders,0))::NUMERIC, 2)
				, 0) AS CPA,
		COALESCE(
			ROUND((total_sales_generated / NULLIF(total_ad_spend,0))::NUMERIC, 2)
				, 0) AS ROAS
	FROM 
		ads_summary
)

-- 3. Rank ad performance
SELECT
	*,
	CASE 
		WHEN ROAS >= 10 THEN 'Top Tier'
		WHEN ROAS >= 7 THEN 'Star Campaign'
		WHEN ROAS >= 4 THEN 'Profitable'
		WHEN ROAS >= 2.2 THEN 'Low ROI (Optimize)' -- assume profit margin is 45% then Break-Even ROAS is 2.2
	ELSE 'Loss Profit (Close & Define Problem)'
	END AS ad_performance
FROM 
	ads_metrics
ORDER BY
	roas DESC
;