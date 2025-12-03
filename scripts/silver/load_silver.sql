-- =================================================================================
-- Script: Silver Layer Data Load
-- Purpose: Load and transform data from Bronze layer into Silver layer tables.
-- Database: [Your Database Name]
-- Notes:
--   1. Removes duplicates based on business keys.
--   2. Cleans string fields (trims spaces, normalizes values).
--   3. Converts integer/string dates into proper DATETIME format.
--   4. Calculates derived fields where needed (e.g., sales, prices).
--   5. Logs duration of each table load using timestamp variables.
--   6. Prints messages for start/end of each table load for monitoring.
-- Usage:
--   Execute this script after the Bronze layer has been loaded.
-- =================================================================================



-- Script to Exclude the duplicated ids from bronze cust_info table
SELECT 
	*
FROM (
	SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Dup_flag
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
	) AS sub
WHERE sub.Dup_flag = 1
;

-- Check for Unwanted Spaces in String data // Do This to All Strings Fields
SELECT 
    cst_firstname
FROM
    bronze.crm_cust_info
WHERE
    cst_firstname != TRIM(cst_firstname);

-- Check Data consisty in low cardinality fields 
SELECT DISTINCT
    (cst_gndr)
FROM
    bronze.crm_cust_info;

SELECT DISTINCT
    (cst_material_status)
FROM
    bronze.crm_cust_info;


-- The Result Inserted into Silver layer After Remove Records With Duplicated ids, Unwanted Spaces, 
-- Checking low cardinality fileds
SET @start_time = NOW();

TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_material_status, cst_gndr, cst_create_date)
SELECT 
	cst_id,
    cst_key,
	TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
	WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
    ELSE 'n/a' 
    END cst_material_status,
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
    ELSE 'n/a'
    END cst_gndr,
    cst_create_date
FROM (
		SELECT *,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Dup_flag
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
) AS sub
WHERE sub.Dup_flag = 1;




-- Loading silver.crm_prd_info
		TRUNCATE TABLE silver.crm_prd_info;
		INSERT INTO silver.crm_prd_info (
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
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    prd_nm,
    IFNULL(prd_cost, 0) AS prd_cost,
    CASE
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    
    CASE
        WHEN LENGTH(prd_start_dt) = 8 AND prd_start_dt REGEXP '^[0-9]+$'
        THEN STR_TO_DATE(prd_start_dt, '%Y%m%d')
        ELSE NULL
    END AS prd_start_dt,

    CASE
        WHEN LENGTH(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) = 8
         AND LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) REGEXP '^[0-9]+$'
        THEN STR_TO_DATE(
             LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
             '%Y%m%d'
        ) - INTERVAL 1 DAY
        ELSE NULL
    END AS prd_end_dt

FROM bronze.crm_prd_info;





TRUNCATE TABLE silver.crm_sales_details;
		INSERT INTO silver.crm_sales_details (
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
			CASE 
				WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
				ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), "%Y%m%d")
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
				ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), "%Y%m%d")
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
				ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), "%Y%m%d")
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_details;
        SET @end_time = NOW();


        -- Loading erp_cust_az12
		TRUNCATE TABLE silver.erp_cust_az12;
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > NOW() THEN NULL
				ELSE bdate
			END AS bdate, -- Set future birthdates to NULL
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12;

        -- Loading erp_loc_a101
		TRUNCATE TABLE silver.erp_loc_a101;
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
		SELECT
			REPLACE(cid, '-', '') AS cid, 
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101;
	    SET @end_time = NOW();
        
        
		-- Loading erp_px_cat_g1v2
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		INSERT INTO silver.erp_px_cat_g1v2 (
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
		FROM bronze.erp_px_cat_g1v2;


SET @end_time = NOW();
SELECT 
	"Time Taken Inserting Silver: ",
    TIMESTAMPDIFF(MICROSECOND, @start_time, @end_time) / 1000 AS ms;




-- CHECKING SILVER QUALITY
SELECT 
    cst_id, COUNT(cst_id)
FROM
    silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(cst_id) > 1 OR cst_id IS NULL;

SELECT 
    cst_firstname
FROM
    silver.crm_cust_info
WHERE
    cst_firstname != TRIM(cst_firstname);

SELECT DISTINCT
    (cst_gndr)
FROM
    silver.crm_cust_info;

SELECT DISTINCT
    (cst_material_status)
FROM
    silver.crm_cust_info;
