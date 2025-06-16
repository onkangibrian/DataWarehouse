use silver;

drop table if exists silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);

drop table if exists silver.crm_cust_info;

drop table if exists silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id       INT,
    cat_id       NVARCHAR(50),
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE,
    dwh_create_date DATETIME default CURRENT_TIMESTAMP
);

drop table if exists silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
   dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

drop table if exists silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50),
    dwh_create_date DATETIME default CURRENT_TIMESTAMP()
);

drop table if exists silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50),
    dwh_create_date DATETIME default CURRENT_TIMESTAMP()
);


drop table if exists silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
    dwh_create_date DATETIME2 default GETDATE()
);

-- inserting clean data into silver layer after having a look at the bronze layer

use silver;
truncate table crm_cust_info;








-- checking out for duplicates or null values
select cst_id,
count(*)
from silver.crm_cust_info cci 
group by cst_id
having count(*) > 1 or cst_id is null;

select cst_lastname 
from silver.crm_cust_info cci 
where cst_lastname !=trim(cst_lastname);


-- Data Standardization & Consistency

select distinct cst_gndr 
from silver.crm_cust_info cci;


select * from silver.crm_cust_info cci ;
	
-- Turning strict mode temporarily

SET SESSION sql_mode = '';
SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION';

-- clean data migrated from the bronze layer cust_info
insert into silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date) 	select
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
from bronze.crm_cust_info cci where cst_id is not null)t where flag_last =1;
	
	

select * from silver.crm_cust_info; -- confirmation of the migration

-- --------------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------------


-- clean data migrated from bronze layer crm_prd_info
insert into silver.crm_prd_info (
	prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
select 
prd_id,
replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id, -- extract Category Id
substring(prd_key,7,char_length(prd_key)) as prd_key, -- extract Product Key
prd_nm,
prd_cost,
case upper(trim(prd_line))
	 when 'M' then 'Mountain'
	 when 'R' then 'Road'
	 when 'S' then 'Other Sales'
	 when 'T' then 'Touring'
	 else 'n/a'
end as prd_line, -- map product line codes to descriptive values; running some normalizations
cast(prd_start_dt as date) as prd_start_dt, -- data type casting; transforming one data type to another
cast(DATE_SUB(
  LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
  INTERVAL 1 day) as date) AS prd_end_dt -- Calculate end dates as one day before the next start date
from bronze.crm_prd_info cpi;


select * from silver.crm_prd_info cpi ;



insert into silver.crm_sales_details (
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
end as sls_sales, -- recalculate sales if original value is missing or incorrect
sls_quantity,
case when sls_price is null or sls_price <= 0
		then sls_sales / nullif(sls_quantity,0)
	 else sls_price
end as sls_price
from bronze.crm_sales_details;

select * from silver.crm_sales_details;

-- ----------------------------------------------------------------------------------------
-- ----------------------------------------------------------------------------------------

-- Building The Silver - Clean & Load erp_cust -a212
use bronze;
select 
case when cid like 'NAS%' then substring(cid,4,CHARACTER_LENGTH(cid)) -- Remove NAS prefix if present
	 else cid
end as cid,
case when bdate > curdate() then null
	 else bdate
end as bdate, -- set future dates to null
CASE 
    WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
    ELSE 'N/A'
END AS gen -- normalize gender values and handle unknown cases
from bronze.erp_cust_az12;



insert into silver.erp_cust_az12 (cid,bdate,gen)
select 
case when cid like 'NAS%' then substring(cid,4,CHARACTER_LENGTH(cid))
	 else cid
end as cid,
case when bdate > curdate() then null
	 else bdate
end as bdate,
CASE 
    WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
    ELSE 'N/A'
END AS gen
from bronze.erp_cust_az12;



select * from silver.erp_cust_az12;

-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- Building The Silver - Clean & Load erp_loc_a101

insert into silver.erp_loc_a101 (cid,cntry)
select 
replace(cid,'-','') cid,
case when trim(cntry) = 'DE' then 'Germany'
	 when trim(cntry) in ('US','USA') then 'United States'
	 when trim(cntry) = '' or cntry is null then 'n/a'
	 else trim(cntry)
end as cntry
from bronze.erp_loc_a101;

select * from silver.erp_loc_a101 ela;

UPDATE silver.erp_loc_a101 ela 
SET cntry = 'Germany'
WHERE cntry = 'DE';

UPDATE silver.erp_loc_a101 ela 
SET cntry = 'United States'
WHERE cntry = 'US';

UPDATE silver.erp_loc_a101 ela 
SET cntry = 'N/A'
WHERE cntry is null;

select * from silver.erp_loc_a101;

-- -----------------------------------------------------------------------------------
-- -----------------------------------------------------------------------------------
-- Building The Silver - Clean & Load erp_px_cat_g1v2
select
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2;

-- check for unwanted spaces
select * 
from bronze.erp_px_cat_g1v2 epcgv 
where cat!= trim(cat) or subcat!= trim(subcat) or maintenance!= trim(maintenance);

-- data standardization and consistency
select distinct 
maintenance
from bronze.erp_px_cat_g1v2; 

insert into silver.erp_px_cat_g1v2 
(id,cat,subcat,maintenance)
select
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2;

select * from silver.erp_px_cat_g1v2 epcgv;

