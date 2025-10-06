
-- Insert latest customer rows into secondary_layer.crm_cust_info.
-- Keeps only the most recent record per cst_id; trims names and normalizes marital status & gender.
INSERT INTO secondary_layer.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
    cst_id,
    cst_key,
    trim(cst_firstname)                      AS cst_firstname,
    trim(cst_lastname)                       AS cst_lastname,
    CASE
        WHEN upper(cst_marital_status) = 'M' THEN 'Married'
        WHEN upper(cst_marital_status) = 'S' THEN 'Single'
        ELSE 'n/a'
    END                                      AS cst_marital_status,
    CASE
        WHEN upper(cst_gndr) = 'F' THEN 'Female'
        WHEN upper(cst_gndr) = 'M' THEN 'Male'
        ELSE 'n/a'
    END                                      AS cst_gndr,
    cst_create_date
FROM (
    SELECT
        *,
        row_number() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
    FROM primary_layer.crm_cust_info
    WHERE cst_id IS NOT NULL
) sub
WHERE rn = 1
;

--------------------------------------------------------------------------------
-- Insert product master into secondary_layer.crm_prd_info.
-- Derives cat_id and prd_key from original key, normalizes line, sets start/end dates, fills missing costs with 0.
WITH src AS (
    SELECT
        prd_id,
        prd_key            AS orig_prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt
    FROM primary_layer.crm_prd_info
)
INSERT INTO secondary_layer.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    prd_id,
    replace(substring(orig_prd_key FROM 1 FOR 5), '-', '_')             AS cat_id,      -- category id
    substring(orig_prd_key FROM 7)                                       AS prd_key,     -- product key part
    prd_nm,
    COALESCE(prd_cost, 0)                                                AS prd_cost,    -- default 0 if null
    CASE
        WHEN upper(trim(prd_line)) = 'M' THEN 'Mountain'
        WHEN upper(trim(prd_line)) = 'R' THEN 'Road'
        WHEN upper(trim(prd_line)) = 'S' THEN 'Other Sales'
        WHEN upper(trim(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END                                                                   AS prd_line,
    prd_start_dt::date                                                    AS prd_start_dt,
    (LEAD(prd_start_dt) OVER (PARTITION BY orig_prd_key ORDER BY prd_start_dt)::date - 1)::date AS prd_end_dt
FROM src
;

--------------------------------------------------------------------------------
-- Insert cleaned sales details into secondary_layer.crm_sales_details.
-- Converts yyyymmdd ints to dates, recalculates sales if inconsistent, and computes price when missing.
WITH src AS (
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    FROM primary_layer.crm_sales_details
),
computed AS (
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
            WHEN sls_order_dt::text = '0' OR length(sls_order_dt::text) <> 8 THEN NULL
            ELSE to_date(sls_order_dt::text, 'YYYYMMDD')
        END AS sls_order_dt,
        CASE
            WHEN sls_ship_dt::text = '0' OR length(sls_ship_dt::text) <> 8 THEN NULL
            ELSE to_date(sls_ship_dt::text, 'YYYYMMDD')
        END AS sls_ship_dt,
        CASE
            WHEN sls_due_dt::text = '0' OR length(sls_due_dt::text) <> 8 THEN NULL
            ELSE to_date(sls_due_dt::text, 'YYYYMMDD')
        END AS sls_due_dt,
        sls_quantity,
        sls_price,
        CASE
            WHEN sls_sales IS NULL
              OR sls_sales <= 0
              OR (sls_quantity IS NOT NULL AND sls_price IS NOT NULL AND sls_sales <> sls_quantity * abs(sls_price))
            THEN sls_quantity * abs(COALESCE(sls_price, 0))
            ELSE sls_sales
        END AS corrected_sales
    FROM src
)
INSERT INTO secondary_layer.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    corrected_sales AS sls_sales,
    sls_quantity,
    CASE
        WHEN sls_price IS NULL OR sls_price <= 0 THEN
            /* avoid division by zero; will return NULL if quantity = 0 */
            corrected_sales::numeric / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM computed
;

--------------------------------------------------------------------------------
-- Insert cleaned ERP customer data into secondary_layer.erp_cust_az12.
-- Removes 'NAS' prefix, nulls future birthdates, and normalizes gender values.
INSERT INTO secondary_layer.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN substring(cid FROM 4)
        ELSE cid
    END AS cid,
    CASE
        WHEN bdate > CURRENT_DATE THEN NULL
        ELSE bdate
    END AS bdate,
    CASE
        WHEN upper(trim(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN upper(trim(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM primary_layer.erp_cust_az12
;

--------------------------------------------------------------------------------
-- Insert normalized ERP locations into secondary_layer.erp_loc_a101.
-- Removes dashes from cid, expands common country codes, and normalizes empty/null countries to 'n/a'.
INSERT INTO secondary_layer.erp_loc_a101 (
    cid,
    cntry
)
SELECT
    replace(cid, '-', '') AS cid,
    CASE
        WHEN trim(cntry) = 'DE' THEN 'Germany'
        WHEN trim(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN cntry IS NULL OR trim(cntry) = '' THEN 'n/a'
        ELSE trim(cntry)
    END AS cntry
FROM primary_layer.erp_loc_a101
;

--------------------------------------------------------------------------------
-- Copy product category reference table into secondary_layer.erp_px_cat_g1v2.
-- Straightforward copy: preserves id, category, subcategory and maintenance columns as-is.
INSERT INTO secondary_layer.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM primary_layer.erp_px_cat_g1v2
;
