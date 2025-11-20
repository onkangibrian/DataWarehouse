-- ============================================
-- STEP 1: Create Database and Tables
-- ============================================

DROP DATABASE IF EXISTS Datawarehouse;
CREATE DATABASE Datawarehouse;
USE Datawarehouse;

CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

USE bronze;

-- Bronze Tables
DROP TABLE IF EXISTS crm_cust_info;
CREATE TABLE crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);

DROP TABLE IF EXISTS crm_prd_info;
CREATE TABLE crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);

DROP TABLE IF EXISTS crm_sales_details;
CREATE TABLE crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales DECIMAL(10,2),
    sls_quantity INT,
    sls_price DECIMAL(10,2)
);

DROP TABLE IF EXISTS erp_loc_a101;
CREATE TABLE erp_loc_a101 (
    cid VARCHAR(50),
    cntry VARCHAR(50)
);

DROP TABLE IF EXISTS erp_cust_az12;
CREATE TABLE erp_cust_az12 (
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(50)
);

DROP TABLE IF EXISTS erp_px_cat_g1v2;
CREATE TABLE erp_px_cat_g1v2 (
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50)
);

-- Log Table
DROP TABLE IF EXISTS load_log;
CREATE TABLE load_log (
    table_name VARCHAR(100),
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50),
    message TEXT
);

-- ============================================
-- STEP 2: Enable Local File Loading
-- ============================================
SET GLOBAL local_infile = 1;

-- ============================================
-- STEP 3: Load Data and Log
-- ============================================

SET @start_time = NOW();

-- Load CRM Customer Info
TRUNCATE TABLE crm_cust_info;
LOAD DATA LOCAL INFILE 'C:/Users/pc/Desktop/dw/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
INTO TABLE crm_cust_info
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
INSERT INTO load_log(table_name, status, message)
VALUES('crm_cust_info','SUCCESS','Loaded successfully');

-- Load CRM Product Info
TRUNCATE TABLE crm_prd_info;
LOAD DATA LOCAL INFILE 'C:/Users/pc/Desktop/dw/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
INTO TABLE crm_prd_info
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
INSERT INTO load_log(table_name, status, message)
VALUES('crm_prd_info','SUCCESS','Loaded successfully');

-- Load CRM Sales Details
TRUNCATE TABLE crm_sales_details;
LOAD DATA LOCAL INFILE 'C:/Users/pc/Desktop/dw/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
INTO TABLE crm_sales_details
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
INSERT INTO load_log(table_name, status, message)
VALUES('crm_sales_details','SUCCESS','Loaded successfully');

-- Load ERP Location
TRUNCATE TABLE erp_loc_a101;
LOAD DATA LOCAL INFILE 'C:/Users/pc/Desktop/dw/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
INTO TABLE erp_loc_a101
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
INSERT INTO load_log(table_name, status, message)
VALUES('erp_loc_a101','SUCCESS','Loaded successfully');

-- Load ERP Customer
TRUNCATE TABLE erp_cust_az12;
LOAD DATA LOCAL INFILE 'C:/Users/pc/Desktop/dw/sql-data-warehouse-project/datasets/source_erp/cust_az12.csv'
INTO TABLE erp_cust_az12
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
INSERT INTO load_log(table_name, status, message)
VALUES('erp_cust_az12','SUCCESS','Loaded successfully');

-- Load ERP PX_CAT_G1V2
TRUNCATE TABLE erp_px_cat_g1v2;
LOAD DATA LOCAL INFILE 'C:/Users/pc/Desktop/dw/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE erp_px_cat_g1v2
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
INSERT INTO load_log(table_name, status, message)
VALUES('erp_px_cat_g1v2','SUCCESS','Loaded successfully');

SET @end_time = NOW();
SET @duration = TIME_TO_SEC(TIMEDIFF(@end_time, @start_time));

-- Total Execution Log
INSERT INTO load_log(table_name, status, message)
VALUES('TOTAL_EXECUTION', 'INFO', CONCAT('Total time (seconds): ', @duration));

-- ============================================
-- STEP 4: Display Summary
-- ============================================

SELECT '===== DATA LOAD SUMMARY =====' AS info;
SELECT table_name, status, message FROM load_log;

SELECT TABLE_NAME, TABLE_ROWS AS estimated_rows
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'bronze';

#Dropping the invalid Date Values from the Customer Table
DELETE FROM bronze.crm_cust_info
WHERE CAST(cst_create_date AS CHAR) = '0000-00-00';
SELECT * FROM bronze.crm_cust_info WHERE CAST(cst_create_date AS CHAR) = '0000-00-00';




