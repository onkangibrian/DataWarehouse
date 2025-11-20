DELIMITER $$

DROP PROCEDURE IF EXISTS silver.load_silver$$

CREATE PROCEDURE silver.load_silver()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;

    -- Start batch
    SET start_time = NOW();
    SELECT '================= Starting Silver Layer Load =================' AS msg;

    -- Load CRM Customer Info--
    SELECT '>> Loading crm_cust_info...' AS msg;
    TRUNCATE TABLE silver.crm_cust_info;

    INSERT INTO silver.crm_cust_info (
        cst_id, cst_key, cst_firstname, cst_lastname,
        cst_marital_status, cst_gndr, cst_create_date, dwh_create_date
    )
    SELECT
        cst_id,
        TRIM(REPLACE(REPLACE(cst_key,'\r',''),'\n','')) AS cst_key,
        TRIM(REPLACE(REPLACE(cst_firstname,'\r',''),'\n','')) AS cst_firstname,
        TRIM(REPLACE(REPLACE(cst_lastname,'\r',''),'\n','')) AS cst_lastname,
        CASE
            WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE
            WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        CASE WHEN cst_create_date IS NULL OR cst_create_date='0000-00-00' THEN NULL ELSE cst_create_date END AS cst_create_date,
        NOW() AS dwh_create_date
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL;

    -- Load CRM Product Info
    SELECT '>> Loading crm_prd_info...' AS msg;
    TRUNCATE TABLE silver.crm_prd_info;

    INSERT INTO silver.crm_prd_info (
        prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt, dwh_create_date
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
        SUBSTRING(prd_key,7) AS prd_key,
        TRIM(REPLACE(REPLACE(prd_nm,'\r',''),'\n','')) AS prd_nm,
        IFNULL(prd_cost,0) AS prd_cost,
        CASE
            WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CASE WHEN prd_start_dt IS NULL OR prd_start_dt='0000-00-00' THEN NULL ELSE prd_start_dt END AS prd_start_dt,
        LEAD(CASE WHEN prd_start_dt IS NULL OR prd_start_dt='0000-00-00' THEN NULL ELSE prd_start_dt END)
            OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL 1 DAY AS prd_end_dt,
        NOW() AS dwh_create_date
    FROM bronze.crm_prd_info;

    									-- Load CRM Sales Details --
    
    SELECT '>> Loading crm_sales_details...' AS msg;
    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id,
        sls_order_dt, sls_ship_dt, sls_due_dt,
        sls_sales, sls_quantity, sls_price, dwh_create_date
    )
    SELECT
        TRIM(REPLACE(REPLACE(sls_ord_num,'\r',''),'\n','')) AS sls_ord_num,
        TRIM(REPLACE(REPLACE(sls_prd_key,'\r',''),'\n','')) AS sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt IS NULL OR sls_order_dt='0000-00-00' THEN NULL ELSE sls_order_dt END AS sls_order_dt,
        CASE WHEN sls_ship_dt IS NULL OR sls_ship_dt='0000-00-00' THEN NULL ELSE sls_ship_dt END AS sls_ship_dt,
        CASE WHEN sls_due_dt IS NULL OR sls_due_dt='0000-00-00' THEN NULL ELSE sls_due_dt END AS sls_due_dt,
        CASE
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity,0)
            ELSE sls_price
        END AS sls_price,
        NOW() AS dwh_create_date
    FROM bronze.crm_sales_details;

    -- Load ERP Customer AZ12
    SELECT '>> Loading erp_cust_az12...' AS msg;
    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12 (
    cid, bdate, gen, dwh_create_date
)
SELECT
    CASE 
        WHEN LEFT(TRIM(REPLACE(REPLACE(cid,'\r',''),'\n','')),3)='NAS' 
        THEN SUBSTRING(TRIM(REPLACE(REPLACE(cid,'\r',''),'\n','')),4)
        ELSE TRIM(REPLACE(REPLACE(cid,'\r',''),'\n',''))
    END AS cid,
    CASE WHEN bdate IS NULL OR bdate='0000-00-00' THEN NULL ELSE bdate END AS bdate,
    CASE
        WHEN UPPER(TRIM(REPLACE(REPLACE(gen,'\r',''),'\n',''))) LIKE 'F%' THEN 'Female'
        WHEN UPPER(TRIM(REPLACE(REPLACE(gen,'\r',''),'\n',''))) LIKE 'M%' THEN 'Male'
        ELSE 'n/a'
    END AS gen,
    NOW() AS dwh_create_date
FROM bronze.erp_cust_az12;


    -- Load ERP Location A101
    SELECT '>> Loading erp_loc_a101...' AS msg;
    TRUNCATE TABLE silver.erp_loc_a101;

    INSERT INTO silver.erp_loc_a101 (
        cid, cntry, dwh_create_date
    )
    SELECT
        REPLACE(TRIM(REPLACE(REPLACE(cid,'\r',''),'\n','')),'-','') AS cid,
        CASE
            WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) IN ('DE') THEN 'Germany'
            WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) IN ('US','USA') THEN 'United States'
            WHEN TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n','')) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(REPLACE(REPLACE(cntry,'\r',''),'\n',''))
        END AS cntry,
        NOW() AS dwh_create_date
    FROM bronze.erp_loc_a101;

    -- Load ERP PX Cat G1V2
    SELECT '>> Loading erp_px_cat_g1v2...' AS msg;
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2 (
        id, cat, subcat, maintenance, dwh_create_date
    )
    SELECT
        TRIM(REPLACE(REPLACE(id,'\r',''),'\n','')) AS id,
        TRIM(REPLACE(REPLACE(cat,'\r',''),'\n','')) AS cat,
        TRIM(REPLACE(REPLACE(subcat,'\r',''),'\n','')) AS subcat,
        TRIM(REPLACE(REPLACE(maintenance,'\r',''),'\n','')) AS maintenance,
        NOW() AS dwh_create_date
    FROM bronze.erp_px_cat_g1v2;

    -- End batch
    SET end_time = NOW();
    SELECT CONCAT('================= Silver Layer Load Completed in ',
                  TIMESTAMPDIFF(SECOND,start_time,end_time),
                  ' seconds =================') AS msg;

END$$

DELIMITER ;
