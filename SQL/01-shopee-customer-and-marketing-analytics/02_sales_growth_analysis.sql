-- ==========================================
-- E-Commerce Sales Growth & Running Total
-- ==========================================
-- Business Question: 
-- How is our revenue growing month-over-month (MoM)?
-- What is our cumulative revenue (Running Total) over time?

select *
FROM order_all_real;

-- 1. Summarize sales and orders by month
WITH monthly_sales AS (
	SELECT
	   	DATE_TRUNC('month', order_date::DATE) AS month,
	    SUM(net_selling_price) AS total_sales,
		COUNT(DISTINCT order_id) AS total_orders
	FROM 
		order_all_real
	WHERE 
		order_status = 'สำเร็จแล้ว' -- delete cancelled and on-going orders
	GROUP BY 
		1
)

-- 2.Calculate MoM sales growth(%) and cummulative revenue over time
SELECT
	month,
	total_sales,
	LAG(total_sales) OVER (ORDER BY month) AS previous_month_sales,
	ROUND(
			(
				(total_sales - LAG(total_sales) OVER (ORDER BY month))/ NULLIF(LAG(total_sales) OVER (ORDER BY month),0) * 100
			):: NUMERIC, -- fixed data type from calculation
		2) AS monthly_growth_pct,
	total_orders,
	SUM(total_sales) OVER (ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_sales
FROM 
	monthly_sales
ORDER BY
	month DESC;