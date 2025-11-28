/*
============================================================
  Data Warehouse Initialization Script
============================================================
  Purpose:
    - Create the three layers of the Data Warehouse
      using Medallion Architecture:
        1. Bronze Layer  (raw data)
        2. Silver Layer  (cleaned/transformed data)
        3. Gold Layer    (analytical/star schema)
    - Ensure databases are created only if they don't exist
    - Serves as the starting point for ETL pipelines
============================================================
*/

-- Creating the Bronze layer database
CREATE DATABASE IF NOT EXISTS bronze;

-- Creating the Silver layer database
CREATE DATABASE IF NOT EXISTS silver;

-- Creating the Gold layer database
CREATE DATABASE IF NOT EXISTS gold;
