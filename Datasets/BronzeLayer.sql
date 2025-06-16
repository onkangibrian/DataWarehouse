create database DataWarehouse;
use DataWarehouse;
create schema bronze;
create schema silver;
create schema gold;

use bronze;


/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);
GO

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
GO

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT null;
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
go


LOAD data local INFILE 'C:/Users/brian/OneDrive/Desktop/dt/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE erp_px_cat_g1v2
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;



-- SHOW VARIABLES LIKE 'local_infile';
-- SET GLOBAL local_infile = 1;

select * from bronze.erp_px_cat_g1v2 epcgv  limit 50;


-- checking out for duplicates or null values
select cst_id,
count(*)
from bronze.crm_cust_info cci 
group by cst_id
having count(*) > 1 or cst_id is null;

-- cleaning values by removing anomalies

select * from(
select *, 
row_number () over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info cci)t where flag_last =1;


select * from(
select *, 
row_number () over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info cci)t where flag_last =1 and cst_id=29466;


-- check for unwanted spaces & cleaning the data
select cst_lastname  from bronze.crm_cust_info cci
where cst_lastname != trim(cst_lastname);

select
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case when upper(trim(cst_marital_status)) = 'M' then 'Married'
	 when upper(trim(cst_marital_status)) = 'S' then 'Single'
	 else 'n/a'
end cst_marital_status,
case when upper(trim(cst_gndr)) = 'F' then 'Female'
	 when upper(trim(cst_gndr)) = 'M' then 'Male'
	 else 'n/a'
end cst_gndr,
cst_create_date
from(
select *, 
row_number () over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info cci where cst_id is not null)t where flag_last = 1;


select cst_key
from bronze.crm_cust_info cci 
where cst_key !=trim(cst_key);


-- Data Standardization & Consistency

select distinct cst_marital_status 
from bronze.crm_cust_info cci;


select * from bronze.crm_cust_info cci ;


SELECT cst_create_date
FROM bronze.crm_cust_info cci
WHERE cst_create_date IS NOT NULL
  AND cst_create_date <> '0000-00-00'
ORDER BY cst_create_date ASC;


delete from bronze.crm_cust_info 
where cst_create_date = '0000-00-00';

select * from bronze.crm_cust_info cci 
where cst_create_date = '0000-00-00';

SET SESSION sql_mode = '';
-- or safer version
SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION';


select 
prd_id,
replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
substring(prd_key,7,char_length(prd_key)) as prd_key,
prd_nm,
prd_cost,
case upper(trim(prd_line))
	 when 'M' then 'Mountain'
	 when 'R' then 'Road'
	 when 'S' then 'Other Sales'
	 when 'T' then 'Touring'
	 else 'n/a'
end as prd_line,
cast(prd_start_dt as date) as prd_start_dt,
cast(DATE_SUB(
  LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
  INTERVAL 1 day) as date) AS prd_end_dt
from bronze.crm_prd_info cpi;

where 
substring(prd_key,7,char_length(prd_key))  in (
select sls_prd_key from bronze.crm_sales_details);


use bronze;
select prd_cost
from bronze.crm_prd_info cpi 
where prd_cost < 0 or prd_cost = 0;

select distinct prd_line
from bronze.crm_prd_info cpi;

select *
from bronze.crm_prd_info cpi
where prd_end_dt < prd_start_dt;

select
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
DATE_SUB(
  LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
  INTERVAL 1 DAY
) AS prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509');




-- bronze sales details  clean up
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or CHAR_LENGTH(sls_order_dt) != 8 then null
	 else str_to_date(cast(sls_order_dt as char ), '%Y%m%d')
end as sls_order_dt,
case when sls_ship_dt = 0 or CHAR_LENGTH(sls_ship_dt) != 8 then null
	 else str_to_date(cast(sls_ship_dt as char ), '%Y%m%d')
end as sls_ship_dt,
case when sls_due_dt = 0 or CHAR_LENGTH(sls_due_dt) != 8 then null
	 else str_to_date(cast(sls_due_dt as char ), '%Y%m%d')
end as sls_due_dt,
case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
		then sls_quantity * abs(sls_price)
	 else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price <= 0
		then sls_sales / nullif(sls_quantity,0)
	 else sls_price
end as sls_price
from bronze.crm_sales_details;

select * from bronze.crm_sales_details csd ;



select * from bronze.erp_loc_a101 ela;

ALTER TABLE bronze.erp_loc_a101 
MODIFY COLUMN cid varchar(50);

ALTER TABLE bronze.erp_loc_a101 
MODIFY COLUMN cntry varchar(50);

UPDATE bronze.erp_loc_a101
SET cntry = 'Germany'
WHERE cntry = 'DE';

UPDATE bronze.erp_loc_a101
SET cntry = 'United States'
WHERE cntry = 'US';

select * from bronze.erp_loc_a101;

