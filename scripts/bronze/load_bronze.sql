-- =========================================================================
-- Script Name: Bronze Layer Data Load
-- Purpose: Load CRM and ERP source CSV data into Bronze tables
-- Description:
--     - Truncate and load data into bronze layer tables
--     - Handle NULL values and basic data cleansing
--     - Measure execution time for each step
-- Notes:
--     - MySQL syntax for LOAD DATA INFILE
--     - Adjust file paths and table names as needed
-- =========================================================================

-- ============================
-- Start timer
-- ============================
SET @start_time = NOW();

-- ============================
-- Use the correct database
-- ============================
USE bronze;

-- =================================================================
-- 1. Load crm_cust_info
-- =================================================================
TRUNCATE TABLE bronze.crm_cust_info;

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/data-warehouse-project/source_crm/cust_info.csv"
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@cst_id, @cst_key, @cst_firstname, @cst_lastname, @cst_material_status, @cst_gndr, @cst_create_date)
SET
    cst_id = NULLIF(@cst_id,''),
    cst_key = NULLIF(@cst_key,''),
    cst_firstname = NULLIF(@cst_firstname,''),
    cst_lastname = NULLIF(@cst_lastname,''),
    cst_material_status = NULLIF(@cst_material_status,''),
    cst_gndr = NULLIF(@cst_gndr,''),
    cst_create_date = NULLIF(REGEXP_REPLACE(@cst_create_date, '[[:space:]]+', ''), '');

-- =================================================================
-- 2. Load crm_prd_info
-- =================================================================
TRUNCATE TABLE bronze.crm_prd_info;

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/data-warehouse-project/source_crm/prd_info.csv"
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 LINES
(@prd_id, @prd_key, @prd_nm, @prd_cost, @prd_line, @prd_start_dt, @prd_end_dt)
SET
    prd_id = NULLIF(TRIM(@prd_id),''),
    prd_key = NULLIF(TRIM(@prd_key),''),
    prd_nm = NULLIF(TRIM(@prd_nm),''),
    prd_cost = NULLIF(TRIM(@prd_cost),''),
    prd_line = NULLIF(TRIM(@prd_line),''),
    prd_start_dt = NULLIF(REGEXP_REPLACE(@prd_start_dt, '[[:space:]]+', ''), ''),
    prd_end_dt = NULLIF(REGEXP_REPLACE(@prd_end_dt, '[[:space:]]+', ''), '');

-- =================================================================
-- 3. Load crm_sales_details
-- =================================================================
TRUNCATE TABLE bronze.crm_sales_details;

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/data-warehouse-project/source_crm/sales_details.csv"
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 LINES
(@sls_ord_num, @sls_prd_key, @sls_cust_id, @sls_order_dt, @sls_shi_dt, @sls_due_dt, @sls_sales, @sls_quantity, @sls_price)
SET
    sls_ord_num = NULLIF(@sls_ord_num, ''),
    sls_prd_key = NULLIF(@sls_prd_key, ''),
    sls_cust_id = NULLIF(@sls_cust_id, ''),
    sls_order_dt = CASE
        WHEN TRIM(@sls_order_dt) IN ('', '0') THEN NULL
        WHEN TRIM(@sls_order_dt) REGEXP '^[0-9]+$'
             AND CAST(@sls_order_dt AS UNSIGNED) BETWEEN 1 AND 73048
             THEN DATE_ADD('1899-12-30', INTERVAL @sls_order_dt DAY)
        WHEN TRIM(@sls_order_dt) REGEXP '^[0-9]{8}$'
             THEN STR_TO_DATE(@sls_order_dt, '%Y%m%d')
        WHEN STR_TO_DATE(TRIM(@sls_order_dt), '%Y-%m-%d') IS NOT NULL
             THEN STR_TO_DATE(TRIM(@sls_order_dt), '%Y-%m-%d')
        ELSE NULL
    END,
    sls_shi_dt = NULLIF(@sls_shi_dt, ''),
    sls_due_dt = NULLIF(@sls_due_dt, ''),
    sls_sales = NULLIF(@sls_sales, ''),
    sls_quantity = NULLIF(@sls_quantity, ''),
    sls_price = NULLIF(REGEXP_REPLACE(@sls_price, '[[:space:]]+', ''), '');

-- =================================================================
-- 4. Load ERP tables (example)
-- =================================================================
TRUNCATE TABLE bronze.erp_cust_az12;
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/data-warehouse-project/source_erp/CUST_AZ12.csv"
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@cid, @bdate, @gen)
SET
    cid = NULLIF(@cid, ''),
    bdate = NULLIF(@bdate, ''),
    gen = NULLIF(@gen, '');

-- (Add other ERP tables the same way...)

-- ============================
-- Stop timer
-- ============================
SET @end_time = NOW();

SELECT 
	"Done Loading Bronze Layer..",
    TIMESTAMPDIFF(MICROSECOND, @start_time, @end_time) AS duration_microseconds,
    TIMESTAMPDIFF(MICROSECOND, @start_time, @end_time)/1000 AS duration_ms,
    TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS duration_seconds;

SELECT 'Source data loading and transformation complete.' AS Status;
