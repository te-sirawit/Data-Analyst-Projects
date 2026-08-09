-- ==============================================================================
-- 📊 MORTGAGE CAPITAL MARKETS DASHBOARD: ETL PIPELINE
-- Engine: DuckDB
-- Description: Transforms raw mortgage loan data into a Star Schema for Power BI.
-- Features: Computes LTV, DTI, Risk Tiers, and Secondary Market Best Execution.
-- ==============================================================================

-- 1. Fact_Trading
-- Calculates Best Execution pricing by comparing Whole Loan Bids vs Securitization (UMBS)
COPY (
    WITH Bids AS (
        SELECT 
            b.loan_id,
            b.golden_sachs, b.storgan_manley, b.smells_largo, b.bank_of_americans, b.pj_logan,
            GREATEST(b.golden_sachs, b.storgan_manley, b.smells_largo, b.bank_of_americans, b.pj_logan) AS max_whole_loan_bid,
            CASE
                WHEN b.golden_sachs >= GREATEST(b.storgan_manley, b.smells_largo, b.bank_of_americans, b.pj_logan) THEN 'Golden Sachs'
                WHEN b.storgan_manley >= GREATEST(b.golden_sachs, b.smells_largo, b.bank_of_americans, b.pj_logan) THEN 'Storgan Manley'
                WHEN b.smells_largo >= GREATEST(b.golden_sachs, b.storgan_manley, b.bank_of_americans, b.pj_logan) THEN 'Smells Largo'
                WHEN b.bank_of_americans >= GREATEST(b.golden_sachs, b.storgan_manley, b.smells_largo, b.pj_logan) THEN 'Bank of Americans'
                ELSE 'PJ Logan'
            END AS best_whole_loan_buyer
        FROM read_csv_auto('raw_data/loan_bids.csv') b
    )
    SELECT 
        l.loan_id,
        TRY_CAST(l.loan_amount AS DOUBLE) AS loan_amount,
        TRY_CAST(l.interest_rate AS DOUBLE) AS interest_rate,
        l.loan_term,
        l.loan_purpose,
        l.closing_date,
        TRY_CAST(u.umbs_price AS DOUBLE) AS securitization_price,
        TRY_CAST(b.max_whole_loan_bid AS DOUBLE) AS max_whole_loan_bid,
        b.best_whole_loan_buyer,
        TRY_CAST(tp.target_profit AS DOUBLE) AS target_profit_usd,
        
        -- Best execution calculation: UMBS vs Whole Loan Bid
        CASE 
            WHEN TRY_CAST(u.umbs_price AS DOUBLE) >= TRY_CAST(b.max_whole_loan_bid AS DOUBLE) THEN 'Securitization (UMBS)'
            ELSE 'Whole Loan Sale'
        END AS best_execution_method,
        GREATEST(TRY_CAST(u.umbs_price AS DOUBLE), TRY_CAST(b.max_whole_loan_bid AS DOUBLE)) AS best_execution_price,
        
        -- Cost and Credit Extraction
        COALESCE(TRY_CAST(l.origition_charges AS DOUBLE), 0) AS origination_charges,
        COALESCE(TRY_CAST(REPLACE(REPLACE(l.lender_credits, '$', ''), ',', '') AS DOUBLE), 0) AS lender_credits,
        
        -- Margin & Profit calculations
        -- Price is in percentage (e.g. 103.5 means 103.5% of loan amount)
        ROUND(((GREATEST(TRY_CAST(u.umbs_price AS DOUBLE), TRY_CAST(b.max_whole_loan_bid AS DOUBLE)) - 100) / 100.0) * TRY_CAST(l.loan_amount AS DOUBLE), 2) AS realized_gross_profit,
        ROUND((((GREATEST(TRY_CAST(u.umbs_price AS DOUBLE), TRY_CAST(b.max_whole_loan_bid AS DOUBLE)) - 100) / 100.0) * TRY_CAST(l.loan_amount AS DOUBLE)) - TRY_CAST(tp.target_profit AS DOUBLE), 2) AS profit_variance
    FROM read_csv_auto('raw_data/loan_data.csv') l
    LEFT JOIN Bids b ON l.loan_id = b.loan_id
    LEFT JOIN read_csv_auto('raw_data/umbs_prices.csv') u ON l.umbs_code = u.umbs_code
    LEFT JOIN read_csv_auto('raw_data/target_profit.csv') tp ON l.loan_id = tp.loan_id
) TO 'processed_data/fact_trading.csv' (HEADER, DELIMITER ',');

-- 2. Dim_Borrower (DTI & Risk Tiers)
COPY (
    SELECT 
        loan_id,
        TRY_CAST(income_thousands AS DOUBLE) * 1000 AS annual_income,
        TRY_CAST(recurring_monthly_debt AS DOUBLE) AS recurring_monthly_debt,
        TRY_CAST(principal_interest_pmt AS DOUBLE) AS principal_interest_pmt,
        TRY_CAST(median_fico_score AS DOUBLE) AS median_fico_score,
        occupancy_type,
        -- DTI = (Monthly Debt + Mortgage P&I) / Monthly Income
        ROUND(((TRY_CAST(recurring_monthly_debt AS DOUBLE) + TRY_CAST(principal_interest_pmt AS DOUBLE)) / ((TRY_CAST(income_thousands AS DOUBLE) * 1000) / 12)) * 100, 2) AS dti_ratio,
        CASE
            WHEN TRY_CAST(median_fico_score AS DOUBLE) >= 740 THEN 'Excellent'
            WHEN TRY_CAST(median_fico_score AS DOUBLE) >= 670 THEN 'Good'
            WHEN TRY_CAST(median_fico_score AS DOUBLE) >= 580 THEN 'Fair'
            ELSE 'Poor'
        END AS fico_tier
    FROM read_csv_auto('raw_data/loan_data.csv')
) TO 'processed_data/dim_borrower.csv' (HEADER, DELIMITER ',');

-- 3. Dim_Property (LTV)
COPY (
    SELECT 
        loan_id,
        state_code,
        county,
        derived_dwelling_category,
        TRY_CAST(property_value AS DOUBLE) AS property_value,
        TRY_CAST(loan_amount AS DOUBLE) AS loan_amount,
        ROUND((TRY_CAST(loan_amount AS DOUBLE) / TRY_CAST(property_value AS DOUBLE)) * 100, 2) AS ltv_ratio,
        CASE 
            WHEN (TRY_CAST(loan_amount AS DOUBLE) / TRY_CAST(property_value AS DOUBLE)) > 0.80 THEN 'High Risk (>80%)'
            ELSE 'Standard Risk (<=80%)'
        END AS ltv_tier
    FROM read_csv_auto('raw_data/loan_data.csv')
) TO 'processed_data/dim_property.csv' (HEADER, DELIMITER ',');

-- 4. Fact_Pipeline
COPY (
    SELECT 
        loan_id,
        TRY_CAST(closing_date AS DATE) AS closing_date,
        TRY_CAST(file_in_audit AS DATE) AS file_in_audit,
        TRY_CAST(file_audit_complete AS DATE) AS file_audit_complete,
        TRY_CAST(file_sent_to_custodian AS DATE) AS file_sent_to_custodian,
        TRY_CAST(file_at_custodian AS DATE) AS file_at_custodian,
        -- Turnaround times (TAT)
        date_diff('day', TRY_CAST(closing_date AS DATE), TRY_CAST(file_audit_complete AS DATE)) AS audit_tat_days,
        date_diff('day', TRY_CAST(file_audit_complete AS DATE), TRY_CAST(file_at_custodian AS DATE)) AS custodian_tat_days
    FROM read_csv_auto('raw_data/loan_status.csv')
) TO 'processed_data/fact_pipeline.csv' (HEADER, DELIMITER ',');
