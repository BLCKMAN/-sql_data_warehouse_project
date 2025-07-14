/*

=============================================

Stored Procedure : Load Bronze Layer ( Source -> Bronze)

=============================================

Script Purpose : 
    This stored procedure loads data into the 'bronze' schema frm external CSV files.
    It perfoms the following actions :
    - Truncates the bronze tables before loading data
    - Uses the 'BULK INSERT' command to load data from csv Files to Bronze tables

Parameters :
    None.
    This stored procedures does not accept any parameters or return any values.

Usage Example :
    EXECC bronze.load_bronze;

=============================================

*/

-- Drop the procedure if it exists
DROP PROCEDURE IF EXISTS bronze.load_bronze();

-- Create the bronze layer loading procedure
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    duration_seconds INTEGER;
BEGIN
    -- Set batch start time
    batch_start_time := CURRENT_TIMESTAMP;
    
    -- Start loading process
    RAISE NOTICE '=====================================';
    RAISE NOTICE 'Load Bronze Layer';
    RAISE NOTICE '=====================================';

    BEGIN -- Start transaction block
        
        -- Load CRM Tables
        RAISE NOTICE '--------------------------------------';
        RAISE NOTICE 'Loading CRM Tables';
        RAISE NOTICE '--------------------------------------';

        -- Load CRM Customer Info
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        
        RAISE NOTICE '>> Loading data from CSV file...';
        COPY bronze.crm_cust_info 
        FROM 'C:\Users\hhlel\Downloads\sql_load-20250621T121121Z-1-001\10k_csv_files\source_crm\cust_info.csv' 
        WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
        
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
        RAISE NOTICE '---------------';

        -- Load CRM Product Info
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        
        RAISE NOTICE '>> Loading data from CSV file...';
        COPY bronze.crm_prd_info 
        FROM 'C:\Users\hhlel\Downloads\sql_load-20250621T121121Z-1-001\10k_csv_files\source_crm\prd_info.csv' 
        WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
        
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
        RAISE NOTICE '---------------';

        -- Load CRM Sales Details
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        
        RAISE NOTICE '>> Loading data from CSV file...';
        COPY bronze.crm_sales_details 
        FROM 'C:\Users\hhlel\Downloads\sql_load-20250621T121121Z-1-001\10k_csv_files\source_crm\sales_details.csv' 
        WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
        
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
        RAISE NOTICE '---------------';

        -- Load ERP Tables
        RAISE NOTICE '--------------------------------------';
        RAISE NOTICE 'Loading ERP Tables';
        RAISE NOTICE '--------------------------------------';

        -- Load ERP Customer AZ12
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        
        RAISE NOTICE '>> Loading data from CSV file...';
        COPY bronze.erp_cust_az12 
        FROM 'C:\Users\hhlel\Downloads\sql_load-20250621T121121Z-1-001\10k_csv_files\source_erp\CUST_AZ12.csv' 
        WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
        
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
        RAISE NOTICE '---------------';

        -- Load ERP Location A101
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        
        RAISE NOTICE '>> Loading data from CSV file...';
        COPY bronze.erp_loc_a101 
        FROM 'C:\Users\hhlel\Downloads\sql_load-20250621T121121Z-1-001\10k_csv_files\source_erp\LOC_A101.csv' 
        WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
        
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
        RAISE NOTICE '---------------';

        -- Load ERP Product Category G1V2
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        
        RAISE NOTICE '>> Loading data from CSV file...';
        COPY bronze.erp_px_cat_g1v2 
        FROM 'C:\Users\hhlel\Downloads\sql_load-20250621T121121Z-1-001\10k_csv_files\source_erp\PX_CAT_G1V2.csv' 
        WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');
        
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Load Duration: % seconds', duration_seconds;
        RAISE NOTICE '---------------';

        -- Calculate total duration
        batch_end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
        
        RAISE NOTICE '==========================';
        RAISE NOTICE 'Loading Bronze Layer is Completed';
        RAISE NOTICE '  - Total Load Duration: % seconds', duration_seconds;
        RAISE NOTICE '==========================';

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '==================================';
            RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
            RAISE NOTICE 'Error Message: %', SQLERRM;
            RAISE NOTICE 'Error Code: %', SQLSTATE;
            RAISE NOTICE '==================================';
            RAISE; -- Re-raise the exception
    END;
END;
$$;

-- Grant execute permission (adjust user as needed)
-- GRANT EXECUTE ON PROCEDURE bronze.load_bronze() TO your_user;

-- To run the procedure, use:
-- CALL bronze.load_bronze();
