/*
========================================================================
-- Database: bronze
-- Purpose : Create Bronze layer tables for CRM and ERP source data
-- Notes   : 
--   1. crm_cust_info  : Stores customer details from CRM
--   2. crm_prd_info   : Stores product details from CRM
--   3. crm_sales_details : Stores sales transaction details from CRM
--   4. erp_cust_az12  : Stores ERP customer AZ12 data
--   5. erp_loc_a101   : Stores ERP location data
--   6. erp_px_cat_g1v2 : Stores ERP price category G1V2 data
-- 
-- Each table uses basic types suitable for bulk inserts and initial loading.
-- DATE and DATETIME types are used for date fields.
-- VARCHAR sizes are set based on expected source CSV data.
========================================================================
*/

-- Switch to the bronze database
USE bronze;

-- ============================
-- 1. CRM Customer Info Table
-- ============================
CREATE TABLE IF NOT EXISTS crm_cust_info (
    cst_id INT,                       -- Customer ID
    cst_key VARCHAR(50),              -- Customer key
    cst_firstname VARCHAR(50),        -- First name
    cst_lastname VARCHAR(50),         -- Last name
    cst_material_status VARCHAR(50),  -- Marital status
    cst_gndr VARCHAR(50),             -- Gender
    cst_create_date DATE              -- Date customer created
);

-- ============================
-- 2. CRM Product Info Table
-- ============================
CREATE TABLE IF NOT EXISTS crm_prd_info (
    prd_id INT,                       -- Product ID
    prd_key VARCHAR(50),               -- Product key
    prd_nm VARCHAR(50),                -- Product name
    prd_cost INT,                      -- Product cost
    prd_line VARCHAR(50),              -- Product line/category
    prd_start_dt DATETIME,             -- Product start date
    prd_end_dt DATETIME                -- Product end date
);

-- ============================
-- 3. CRM Sales Details Table
-- ============================
CREATE TABLE IF NOT EXISTS crm_sales_details (
    sls_ord_num VARCHAR(50),           -- Sales order number
    sls_prd_key VARCHAR(50),           -- Product key
    sls_cust_id INT,                   -- Customer ID
    sls_order_dt DATETIME,             -- Order date
    sls_shi_dt DATETIME,               -- Shipping date
    sls_due_dt DATETIME,               -- Due date
    sls_sales INT,                     -- Sales amount
    sls_quantity INT,                  -- Quantity sold
    sls_price INT                      -- Price per unit
);

-- ============================
-- 4. ERP Customer AZ12 Table
-- ============================
CREATE TABLE IF NOT EXISTS erp_cust_az12 (
    cid VARCHAR(50),                   -- Customer ID
    bdate DATE,                        -- Birthdate
    gen VARCHAR(50)                    -- Gender
);

-- ============================
-- 5. ERP Location Table
-- ============================
CREATE TABLE IF NOT EXISTS erp_loc_a101 (
    cid VARCHAR(50),                   -- Customer ID
    cntry VARCHAR(50)                  -- Country
);

-- ============================
-- 6. ERP Price Category Table
-- ============================
CREATE TABLE IF NOT EXISTS erp_px_cat_g1v2 (
    id VARCHAR(50),                    -- Category ID
    cat VARCHAR(50),                   -- Category
    subcat VARCHAR(50),                -- Subcategory
    maintenance VARCHAR(50)            -- Maintenance flag/notes
);
