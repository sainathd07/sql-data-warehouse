/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    Call Silver.load_silver();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    batch_start_time TIMESTAMP;
    start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    end_time TIMESTAMP;
    load_time NUMERIC;
    batch_load_time NUMERIC;
BEGIN
	RAISE NOTICE '==================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '==================================================';
	
	BEGIN
	 	batch_start_time := clock_timestamp();
	
		RAISE NOTICE '--------------------------------------------------';
		RAISE NOTICE 'Loading CRM Tables';
		RAISE NOTICE '--------------------------------------------------';
	
		start_time := clock_timestamp();
		RAISE NOTICE '>>Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		RAISE NOTICE '>>Inserting data into silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
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
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE
			WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
			WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
			ELSE 'n/a'
		END cst_marital_status, -- Normalize marital status values to readable format
		CASE
			WHEN UPPER(TRIM(cst_gndr))='F' THEN 'FEMALE'
			WHEN UPPER(TRIM(cst_gndr))='M' THEN 'MALE'
			ELSE 'n/a'
		END cst_gndr, -- -- Normalize gender values to readable format
		cst_create_date
		FROM (
		SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_number
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		)t WHERE flag_number = 1; -- Select most recent record per customer
	
		end_time := clock_timestamp();
		load_time := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE 'Load Duration: % seconds', load_time;
		RAISE NOTICE '--------------------------------------------------';
	
		start_time := clock_timestamp();
	
		RAISE NOTICE '>>Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		RAISE NOTICE '>>Inserting data into silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
		prd_id,
		prd_key,
		cat_id,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		SELECT prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
		SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key, -- Extract product Key
		prd_nm,
		COALESCE(prd_cost::INT, 0) AS prd_cost,  -- Convert prd_cost to INT and replace NULL with 0
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END prd_line, -- Map product line codes to descriptive values
		CAST (prd_start_dt AS DATE) AS prd_start_dt,
		CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;
	
		end_time := clock_timestamp();
		load_time := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE 'Load Duration: % seconds', load_time;
		RAISE NOTICE '--------------------------------------------------';
	
		start_time := clock_timestamp();
		
		RAISE NOTICE '>>Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		RAISE NOTICE '>>Inserting data into silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
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
		CASE WHEN sls_order_dt = '0' OR LENGTH(sls_order_dt) != 8 THEN NULL
			ELSE CAST(sls_order_dt AS DATE)
		END sls_order_dt,
		CASE WHEN sls_ship_dt = '0' OR LENGTH(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(sls_ship_dt AS DATE)
		END sls_ship_dt,
		CASE WHEN sls_due_dt = '0' OR LENGTH(sls_due_dt) != 8 THEN NULL
			ELSE CAST(sls_due_dt AS DATE)
		END sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales!=sls_quantity*ABS(sls_price) THEN sls_quantity*ABS(sls_price)
			ELSE sls_sales
		END sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales/NULLIF(sls_quantity, 0)
			ELSE sls_price
		END sls_price
		FROM bronze.crm_sales_details;
	
		end_time := clock_timestamp();
		load_time := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE 'Load Duration: % seconds', load_time;
		RAISE NOTICE '--------------------------------------------------';
	
		RAISE NOTICE '--------------------------------------------------';
		RAISE NOTICE 'Loading ERP Tables';
		RAISE NOTICE '--------------------------------------------------';
	
		start_time := clock_timestamp();
		
		RAISE NOTICE '>>Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		RAISE NOTICE '>>Inserting data into silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)
		SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
			ELSE cid
		END cid,
		CASE WHEN bdate > clock_Timestamp() THEN NULL
			ELSE bdate
		END bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			 ELSE gen
		END gen
		FROM bronze.erp_cust_az12;
	
		end_time := clock_timestamp();
		load_time := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE 'Load Duration: % seconds', load_time;
		RAISE NOTICE '--------------------------------------------------';
	
		start_time := clock_timestamp();
		
		RAISE NOTICE '>>Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		RAISE NOTICE '>>Inserting data into silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
		)
		SELECT
		REPLACE(cid, '-', '') cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN (cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END cntry
		FROM bronze.erp_loc_a101;
	
		end_time := clock_timestamp();
		load_time := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE 'Load Duration: % seconds', load_time;
		RAISE NOTICE '--------------------------------------------------';
	
		start_time := clock_timestamp();
		
		RAISE NOTICE '>>Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		RAISE NOTICE '>>Inserting data into silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
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
	
		end_time := clock_timestamp();
		load_time := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE 'Load Duration: % seconds', load_time;
		RAISE NOTICE '--------------------------------------------------';
	
		EXCEPTION
	        WHEN OTHERS THEN
	            -- Log the error message and error code
	            RAISE NOTICE 'Error occurred while loading silver layer: %', SQLERRM;
	            RAISE NOTICE 'SQLSTATE: %', SQLSTATE;
	END;
 -- Capture the batch end time and calculate the total loading time
batch_end_time := clock_timestamp();
batch_load_time := EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));

-- Log the end of the loading process
RAISE NOTICE '==================================================';
RAISE NOTICE 'Finished Loading Silver Layer';
RAISE NOTICE 'Load Duration of Silver layer: % seconds', batch_load_time;
RAISE NOTICE '==================================================';
END $$;