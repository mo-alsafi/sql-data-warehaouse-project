-- =================================================================================
-- Script: Silver Layer Table Definitions
-- Purpose: Create Silver layer tables for the Data Warehouse.
-- Author: [Your Name]
-- Date: [YYYY-MM-DD]
-- Database: Silver
-- Notes:
--   1. Drops existing tables before creation to ensure schema refresh.
--   2. Defines all necessary tables: crm_cust_info, crm_prd_info, crm_sales_details,
--      erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2.
--   3. Adds 'dwh_create_date' to each table for automatic record timestamping.
--   4. All fields use appropriate data types and lengths based on source data.
-- Usage:
--   Execute this script before inserting data into Silver layer.
-- ===========================================================================

USE silver;

DROP TABLE crm_cust_info;
CREATE TABLE IF NOT EXISTS crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_material_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE crm_prd_info;
CREATE TABLE IF NOT EXISTS crm_prd_info (
	prd_id INT,
    cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);
DROP TABLE crm_sales_details;
CREATE TABLE IF NOT EXISTS crm_sales_details (
	sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATETIME,
    sls_ship_dt DATETIME,
    sls_due_dt DATETIME,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE erp_cust_az12;
CREATE TABLE IF NOT EXISTS erp_cust_az12 (
	cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE erp_loc_a101;
CREATE TABLE IF NOT EXISTS erp_loc_a101 (
	cid VARCHAR(50),
    cntry VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);
DROP TABLE erp_px_cat_g1v2;
CREATE TABLE IF NOT EXISTS erp_px_cat_g1v2 (
	id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50),
    dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
)
