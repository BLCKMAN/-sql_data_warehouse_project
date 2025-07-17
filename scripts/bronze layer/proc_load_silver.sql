-- Enhanced Bronze to Silver ETL Function with Comprehensive Logging and Error Handling
CREATE OR REPLACE FUNCTION bronze_to_silver_etl()
RETURNS void AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    duration_seconds INTEGER;
    total_duration_seconds INTEGER;
    record_count INTEGER;
BEGIN
    -- Set batch start time
    batch_start_time := CURRENT_TIMESTAMP;
    
    -- Start ETL process
    RAISE NOTICE '=====================================';
    RAISE NOTICE 'Bronze to Silver ETL Process';
    RAISE NOTICE '=====================================';

    BEGIN -- Start transaction block
        
        -- Process CRM Sales Details
        RAISE NOTICE '--------------------------------------';
        RAISE NOTICE 'Processing CRM Sales Details';
        RAISE NOTICE '--------------------------------------';
        
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        
        RAISE NOTICE '>> Transforming and loading data...';
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
            TRIM(sls_prd_key) AS sls_prd_key,
            sls_cust_id,
            CASE 
                WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE 
                WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE 
                WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE WHEN sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            ABS(sls_price) AS sls_price
        FROM bronze.crm_sales_details;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Records processed: %', record_count;
        RAISE NOTICE '>> Processing Duration: % seconds', duration_seconds;
        RAISE NOTICE '>> Completed CRM Sales Details processing';
        RAISE NOTICE '---------------';
        
        -- Process CRM Customer Info
        RAISE NOTICE '--------------------------------------';
        RAISE NOTICE 'Processing CRM Customer Info';
        RAISE NOTICE '--------------------------------------';
        
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        
        RAISE NOTICE '>> Transforming and loading data (deduplicating)...';
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
            TRIM(cst_key) AS cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                 ELSE 'n/a'
            END AS cst_marital_status,
            CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                 ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
        ) ranked 
        WHERE flag_last = 1;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Records processed: %', record_count;
        RAISE NOTICE '>> Processing Duration: % seconds', duration_seconds;
        RAISE NOTICE '>> Completed CRM Customer Info processing';
        RAISE NOTICE '---------------';
        
        -- Process CRM Product Info
        RAISE NOTICE '--------------------------------------';
        RAISE NOTICE 'Processing CRM Product Info';
        RAISE NOTICE '--------------------------------------';
        
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        
        RAISE NOTICE '>> Transforming and loading data (calculating end dates)...';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_') AS cat_id,
            SUBSTRING(TRIM(prd_key), 7, LENGTH(TRIM(prd_key))) AS prd_key,
            TRIM(prd_nm) AS prd_nm,
            COALESCE(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sale'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY TRIM(prd_key) ORDER BY prd_start_dt) - INTERVAL '1 day' AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Records processed: %', record_count;
        RAISE NOTICE '>> Processing Duration: % seconds', duration_seconds;
        RAISE NOTICE '>> Completed CRM Product Info processing';
        RAISE NOTICE '---------------';
        
        -- Process ERP Customer AZ12
        RAISE NOTICE '--------------------------------------';
        RAISE NOTICE 'Processing ERP Customer AZ12';
        RAISE NOTICE '--------------------------------------';
        
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        
        RAISE NOTICE '>> Transforming and loading data (cleaning customer IDs)...';
        INSERT INTO silver.erp_cust_az12(
            cid,
            bdate,
            gen
        )
        SELECT
            CASE WHEN TRIM(cid) LIKE 'NAS%' THEN SUBSTRING(TRIM(cid), 4, LENGTH(TRIM(cid)::TEXT))
                ELSE TRIM(cid)
            END AS cid,
            CASE WHEN bdate > CURRENT_DATE THEN NULL
                ELSE bdate
            END AS bdate,
            CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                 ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Records processed: %', record_count;
        RAISE NOTICE '>> Processing Duration: % seconds', duration_seconds;
        RAISE NOTICE '>> Completed ERP Customer AZ12 processing';
        RAISE NOTICE '---------------';
        
        -- Process ERP Location A101
        RAISE NOTICE '--------------------------------------';
        RAISE NOTICE 'Processing ERP Location A101';
        RAISE NOTICE '--------------------------------------';
        
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
        
        RAISE NOTICE '>> Transforming and loading data (standardizing countries)...';
        INSERT INTO silver.erp_loc_a101(
            cid,
            cntry
        )
        SELECT 
            REPLACE(TRIM(cid), '-', '') AS cid,
            CASE 
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Records processed: %', record_count;
        RAISE NOTICE '>> Processing Duration: % seconds', duration_seconds;
        RAISE NOTICE '>> Completed ERP Location A101 processing';
        RAISE NOTICE '---------------';
        
        -- Process ERP PX Category G1V2
        RAISE NOTICE '--------------------------------------';
        RAISE NOTICE 'Processing ERP PX Category G1V2';
        RAISE NOTICE '--------------------------------------';
        
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        
        RAISE NOTICE '>> Transforming and loading data (cleaning categories)...';
        INSERT INTO silver.erp_px_cat_g1v2(
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            TRIM(cat) AS cat,
            TRIM(subcat) AS subcat,
            TRIM(maintenance) AS maintenance
        FROM bronze.erp_px_cat_g1v2;
        
        GET DIAGNOSTICS record_count = ROW_COUNT;
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Records processed: %', record_count;
        RAISE NOTICE '>> Processing Duration: % seconds', duration_seconds;
        RAISE NOTICE '>> Completed ERP PX Category G1V2 processing';
        RAISE NOTICE '---------------';
        
        -- Calculate total duration
        batch_end_time := CURRENT_TIMESTAMP;
        total_duration_seconds := EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
        
        RAISE NOTICE '=====================================';
        RAISE NOTICE 'Bronze to Silver ETL Process Completed Successfully';
        RAISE NOTICE '  - Total Processing Duration: % seconds', total_duration_seconds;
        RAISE NOTICE '=====================================';

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '=====================================';
            RAISE NOTICE 'ERROR OCCURRED DURING BRONZE TO SILVER ETL';
            RAISE NOTICE 'Error Message: %', SQLERRM;
            RAISE NOTICE 'Error Code: %', SQLSTATE;
            RAISE NOTICE 'Error Detail: %', SQLERRM;
            RAISE NOTICE '=====================================';
            RAISE; -- Re-raise the exception
    END;
    
END;
$$ LANGUAGE plpgsql;

-- Enhanced Data Quality Validation Function with Logging and Error Handling
CREATE OR REPLACE FUNCTION validate_bronze_data_quality()
RETURNS void AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    duration_seconds INTEGER;
    total_duration_seconds INTEGER;
    issue_count INTEGER;
BEGIN
    -- Set batch start time
    batch_start_time := CURRENT_TIMESTAMP;
    
    -- Start validation process
    RAISE NOTICE '=====================================';
    RAISE NOTICE 'Data Quality Validation';
    RAISE NOTICE '=====================================';

    BEGIN -- Start transaction block
        
        -- Check CRM Sales Details for calculation mismatches
        RAISE NOTICE '--------------------------------------';
        RAISE NOTICE 'Checking CRM Sales Details';
        RAISE NOTICE '--------------------------------------';
        
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Checking for calculation errors and null values...';
        
        SELECT COUNT(*) INTO issue_count
        FROM bronze.crm_sales_details 
        WHERE sls_sales != sls_quantity * ABS(sls_price)
           OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
           OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;
        
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Issues found: %', issue_count;
        RAISE NOTICE '>> Check Duration: % seconds', duration_seconds;
        RAISE NOTICE '---------------';
        
        -- Check CRM Customer Info for unwanted spaces
        RAISE NOTICE '--------------------------------------';
        RAISE NOTICE 'Checking CRM Customer Info';
        RAISE NOTICE '--------------------------------------';
        
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Checking for space issues...';
        
        SELECT COUNT(*) INTO issue_count
        FROM bronze.crm_cust_info 
        WHERE cst_firstname != TRIM(cst_firstname) 
           OR cst_lastname != TRIM(cst_lastname)
           OR cst_key != TRIM(cst_key);
        
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Issues found: %', issue_count;
        RAISE NOTICE '>> Check Duration: % seconds', duration_seconds;
        RAISE NOTICE '---------------';
        
        -- Check CRM Product Info for space issues
        RAISE NOTICE '--------------------------------------';
        RAISE NOTICE 'Checking CRM Product Info';
        RAISE NOTICE '--------------------------------------';
        
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Checking for space issues...';
        
        SELECT COUNT(*) INTO issue_count
        FROM bronze.crm_prd_info 
        WHERE prd_key != TRIM(prd_key) 
           OR prd_nm != TRIM(prd_nm)
           OR prd_line != TRIM(prd_line);
        
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Issues found: %', issue_count;
        RAISE NOTICE '>> Check Duration: % seconds', duration_seconds;
        RAISE NOTICE '---------------';
        
        -- Check ERP Customer AZ12 for space issues
        RAISE NOTICE '--------------------------------------';
        RAISE NOTICE 'Checking ERP Customer AZ12';
        RAISE NOTICE '--------------------------------------';
        
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Checking for space issues...';
        
        SELECT COUNT(*) INTO issue_count
        FROM bronze.erp_cust_az12 
        WHERE cid != TRIM(cid) 
           OR gen != TRIM(gen);
        
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Issues found: %', issue_count;
        RAISE NOTICE '>> Check Duration: % seconds', duration_seconds;
        RAISE NOTICE '---------------';
        
        -- Check ERP Location A101 for space issues
        RAISE NOTICE '--------------------------------------';
        RAISE NOTICE 'Checking ERP Location A101';
        RAISE NOTICE '--------------------------------------';
        
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Checking for space issues...';
        
        SELECT COUNT(*) INTO issue_count
        FROM bronze.erp_loc_a101 
        WHERE cid != TRIM(cid) 
           OR cntry != TRIM(cntry);
        
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Issues found: %', issue_count;
        RAISE NOTICE '>> Check Duration: % seconds', duration_seconds;
        RAISE NOTICE '---------------';
        
        -- Check ERP PX Category G1V2 for unwanted spaces
        RAISE NOTICE '--------------------------------------';
        RAISE NOTICE 'Checking ERP PX Category G1V2';
        RAISE NOTICE '--------------------------------------';
        
        start_time := CURRENT_TIMESTAMP;
        RAISE NOTICE '>> Checking for space issues...';
        
        SELECT COUNT(*) INTO issue_count
        FROM bronze.erp_px_cat_g1v2 
        WHERE cat != TRIM(cat) 
           OR subcat != TRIM(subcat) 
           OR maintenance != TRIM(maintenance);
        
        end_time := CURRENT_TIMESTAMP;
        duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> Issues found: %', issue_count;
        RAISE NOTICE '>> Check Duration: % seconds', duration_seconds;
        RAISE NOTICE '---------------';
        
        -- Calculate total duration
        batch_end_time := CURRENT_TIMESTAMP;
        total_duration_seconds := EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
        
        RAISE NOTICE '=====================================';
        RAISE NOTICE 'Data Quality Validation Completed Successfully';
        RAISE NOTICE '  - Total Validation Duration: % seconds', total_duration_seconds;
        RAISE NOTICE '=====================================';

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '=====================================';
            RAISE NOTICE 'ERROR OCCURRED DURING DATA QUALITY VALIDATION';
            RAISE NOTICE 'Error Message: %', SQLERRM;
            RAISE NOTICE 'Error Code: %', SQLSTATE;
            RAISE NOTICE 'Error Detail: %', SQLERRM;
            RAISE NOTICE '=====================================';
            RAISE; -- Re-raise the exception
    END;
    
END;
$$ LANGUAGE plpgsql;

-- Usage Examples:
-- SELECT validate_bronze_data_quality();
-- SELECT bronze_to_silver_etl();
