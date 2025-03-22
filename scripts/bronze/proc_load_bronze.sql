/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
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
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '==================================================';

    BEGIN
        batch_start_time := clock_timestamp();

        RAISE NOTICE '--------------------------------------------------';
        RAISE NOTICE 'Loading CRM Tables';
        RAISE NOTICE '--------------------------------------------------';

        start_time := clock_timestamp();

        -- Load crm_cust_info
        RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        RAISE NOTICE '>> Copying Table: bronze.crm_cust_info';
        COPY bronze.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
        FROM '/Users/sainathdushatti/projects/sql-data-warehouse/datasets/source_crm/cust_info.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        load_time := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'Loaded % rows into bronze.crm_cust_info', (SELECT COUNT(*) FROM bronze.crm_cust_info);
        RAISE NOTICE 'Load Duration: % seconds', load_time;
        RAISE NOTICE '--------------------------------------------------';

        start_time := clock_timestamp();

        -- Load crm_prd_info
        RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        RAISE NOTICE '>> Copying Table: bronze.crm_prd_info';
        COPY bronze.crm_prd_info(prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
        FROM '/Users/sainathdushatti/projects/sql-data-warehouse/datasets/source_crm/prd_info.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        load_time := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'Loaded % rows into bronze.crm_prd_info', (SELECT COUNT(*) FROM bronze.crm_prd_info);
        RAISE NOTICE 'Load Duration: % seconds', load_time;
        RAISE NOTICE '--------------------------------------------------';

        start_time := clock_timestamp();

        -- Load crm_sales_details
        RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        RAISE NOTICE '>> Copying Table: bronze.crm_sales_details';
        COPY bronze.crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
        FROM '/Users/sainathdushatti/projects/sql-data-warehouse/datasets/source_crm/sales_details.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        load_time := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'Loaded % rows into bronze.crm_sales_details', (SELECT COUNT(*) FROM bronze.crm_sales_details);
        RAISE NOTICE 'Load Duration: % seconds', load_time;
        RAISE NOTICE '--------------------------------------------------';

        RAISE NOTICE '--------------------------------------------------';
        RAISE NOTICE 'Loading ERP Tables';
        RAISE NOTICE '--------------------------------------------------';

        start_time := clock_timestamp();

        -- Load erp_loc_a101
        RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        RAISE NOTICE '>> Copying Table: bronze.erp_loc_a101';
        COPY bronze.erp_loc_a101(cid, cntry)
        FROM '/Users/sainathdushatti/projects/sql-data-warehouse/datasets/source_erp/LOC_A101.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        load_time := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'Loaded % rows into bronze.erp_loc_a101', (SELECT COUNT(*) FROM bronze.erp_loc_a101);
        RAISE NOTICE 'Load Duration: % seconds', load_time;
        RAISE NOTICE '--------------------------------------------------';

        start_time := clock_timestamp();

        -- Load erp_cust_az12
        RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        RAISE NOTICE '>> Copying Table: bronze.erp_cust_az12';
        COPY bronze.erp_cust_az12(cid, bdate, gen)
        FROM '/Users/sainathdushatti/projects/sql-data-warehouse/datasets/source_erp/CUST_AZ12.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        load_time := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'Loaded % rows into bronze.erp_cust_az12', (SELECT COUNT(*) FROM bronze.erp_cust_az12);
        RAISE NOTICE 'Load Duration: % seconds', load_time;
        RAISE NOTICE '--------------------------------------------------';

        start_time := clock_timestamp();

        -- Load erp_px_cat_g1v2
        RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        RAISE NOTICE '>> Copying Table: bronze.erp_px_cat_g1v2';
        COPY bronze.erp_px_cat_g1v2(id, cat, subcat, maintenance)
        FROM '/Users/sainathdushatti/projects/sql-data-warehouse/datasets/source_erp/PX_CAT_G1V2.csv'
        DELIMITER ','
        CSV HEADER;

        end_time := clock_timestamp();
        load_time := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE 'Loaded % rows into bronze.erp_px_cat_g1v2', (SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2);
        RAISE NOTICE 'Load Duration: % seconds', load_time;

    EXCEPTION
        WHEN OTHERS THEN
            -- Log the error message and error code
            RAISE NOTICE 'Error occurred while loading bronze layer: %', SQLERRM;
            RAISE NOTICE 'SQLSTATE: %', SQLSTATE;
    END;

    -- Capture the batch end time and calculate the total loading time
    batch_end_time := clock_timestamp();
    batch_load_time := EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));

    -- Log the end of the loading process
    RAISE NOTICE '==================================================';
    RAISE NOTICE 'Finished Loading Bronze Layer';
    RAISE NOTICE 'Load Duration of bronze layer: % seconds', batch_load_time;
    RAISE NOTICE '==================================================';
END;
$$;