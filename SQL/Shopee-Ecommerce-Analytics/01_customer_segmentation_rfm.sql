-- ==========================================
-- E-Commerce Customer Segmentation (RFM Analysis)
-- ==========================================
-- Business Question: 
-- Who are our most valuable customers (VIP) and who is at risk of churning?
-- We want to segment customers based on Recency, Frequency, and Monetary (RFM) value.


-- 1. Calculate RFM values (Recency, Frequency, Monetary) by buyer_username
WITH buyer_rfm AS (
	SELECT
		buyer_username,
		MAX(order_date::DATE) AS last_purchase_date,
		COUNT(DISTINCT order_id) AS frequency,
		SUM(net_selling_price) AS monetary_value
	FROM
		order_all_real
	WHERE
		order_status = 'สำเร็จแล้ว' AND -- filter only completed orders
		buyer_username IS NOT NULL -- clean blank values
	GROUP BY 
		buyer_username
),

-- 2. Rank RFM scores from 1(lowest) to 5(highest) using NTILE(5)
rfm_score AS (
	SELECT
		buyer_username,
		last_purchase_date,
		frequency,
		monetary_value,
		CURRENT_DATE - last_purchase_date AS recency_days,
		NTILE(5) OVER(ORDER BY CURRENT_DATE - last_purchase_date DESC) AS r_score,
		NTILE(5) OVER(ORDER BY frequency ASC) AS f_score,
		NTILE(5) OVER(ORDER BY monetary_value ASC) AS m_score
	FROM 
		buyer_rfm
)

-- 3. Categorize customers into segments based on RFM scores
SELECT
	buyer_username,
	recency_days,
	frequency,
	monetary_value,
	r_score,
	f_score,
	m_score,
	CASE 
		WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'VIP'
		WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customer'
		WHEN r_score >= 4 AND f_score <= 2 AND m_score >= 4 THEN 'Promising New Customer'
		WHEN r_score >= 4 AND f_score <= 2 AND m_score <= 2 THEN 'New Customer'
		WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3 THEN 'At Risk (High Value)'
		WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2 THEN 'Lost Customer'
		ELSE 'Regular Customer'
	END AS customer_segment
FROM 
	rfm_score
ORDER BY 
    m_score DESC, f_score DESC, r_score DESC;
;