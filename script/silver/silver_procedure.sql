/*
The silver.load_silver stored procedure performs a complete ETL operation, transforming raw data from multiple bronze-layer tables into cleaned, standardized silver-layer tables. It begins by truncating existing silver data to ensure freshness and avoid duplication. Key transformations include removing duplicates based on latest timestamps, normalizing categorical values (like gender and marital status), converting invalid dates to NULL, recalculating sales figures for consistency, and standardizing string formats (trimming spaces, renaming categories). The procedure also creates derived fields such as product categories and start/end dates. Wrapped in a TRY...CATCH block for error handling, it measures the total execution time and provides resilience in case of load failures.
*/

-- Creating and Altering Stored Procedure
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
		DECLARE @starttime DATETIME2, @endtime DATETIME2;
		SET @starttime = GETDATE();
		TRUNCATE TABLE silver.crm_cust_info;
		WITH no_duplicate AS 
		(SELECT cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gndr,
		cst_create_date,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS ranks
		FROM bronze.crm_cust_info)
		INSERT INTO silver.crm_cust_info (cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gndr,
		cst_create_date)
		SELECT cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE
			WHEN UPPER(cst_material_status) = 'S' THEN 'Single'
			WHEN UPPER(cst_material_status) = 'M' THEN 'Married'
			ELSE 'Undecided'
			END AS cst_marital_status,
		CASE 
			WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
			WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
			ELSE 'Unknown'
			END AS cst_gndr,
		cst_create_date
		FROM no_duplicate
		WHERE ranks = 1 AND cst_id IS NOT NULL;

		TRUNCATE TABLE silver.crm_prd_info;
		INSERT INTO silver.crm_prd_info (
							prd_id,
							cat_id,
							prd_key,
							prd_nm,
							prd_cost,
							prd_line,
							prd_start_date,
							prd_end_date)
		SELECT prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE
				WHEN TRIM(UPPER(prd_line)) = 'M' THEN 'Mountain'
				WHEN TRIM(UPPER(prd_line)) = 'S' THEN 'Other Sales'
				WHEN TRIM(UPPER(prd_line)) = 'R' THEN 'Road'
				WHEN TRIM(UPPER(prd_line)) = 'T' THEN 'Touring'
				ELSE 'Unknown'
				END AS prd_line,
			prd_start_date,
			DATEADD(DAY,-1,LEAD(prd_start_date) OVER(PARTITION BY prd_key ORDER BY prd_start_date ASC)) AS prd_end_date
		FROM bronze.crm_prd_info;

		TRUNCATE TABLE silver.crm_sales_details;
		INSERT INTO silver.crm_sales_details (
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
		SELECT sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CAST(CAST(NULLIF(sls_order_dt,'0') AS VARCHAR) AS DATE) AS sls_order_dt,
			CAST(CAST(NULLIF(sls_ship_dt,'0') AS VARCHAR) AS DATE) AS sls_ship_dt,
			CAST(CAST(NULLIF(sls_due_dt,'0') AS VARCHAR) AS DATE) AS sls_due_dt,
			CASE 
					WHEN sls_sales <= 0 OR sls_sales IS NULL or sls_sales!= sls_quantity *ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales
					END AS sls_sales,
			sls_quantity,
				CASE
					WHEN sls_price IS NULL OR sls_price <=0 
					THEN sls_sales/NULLIF(sls_quantity,0)
					ELSE sls_price
					END AS sls_price
		FROM bronze.crm_sales_details;

		TRUNCATE TABLE silver.erp_CUST_AZ12;
		INSERT INTO silver.erp_CUST_AZ12 (cid,bdate,gen)
		SELECT 
			   CASE 
					WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
					ELSE CID
					END AS CID,
			   CASE 
					WHEN BDATE > GETDATE() OR BDATE < '1920-01-01' THEN NULL
					ELSE BDATE
					END AS BDATE,
			   CASE 
					WHEN UPPER(TRIM(GEN)) LIKE 'M%' THEN 'Male'
					WHEN UPPER(TRIM(GEN)) LIKE 'F%' THEN 'Female'
					ELSE 'Unknown'
					END AS GEN
		FROM bronze.erp_CUST_AZ12;

		TRUNCATE TABLE silver.erp_LOC_A101;
		INSERT INTO silver.erp_LOC_A101 (CID,CNTRY)
		SELECT	TRIM(REPLACE(CID,'-','')) AS CID,
				CASE
					WHEN TRIM(UPPER(CNTRY)) IN ('USA','UNITED STATES','US') THEN 'United States'
					WHEN TRIM(UPPER(CNTRY)) LIKE '%KINGDOM%' THEN 'United Kingdom'
					WHEN TRIM(UPPER(CNTRY)) LIKE 'DE%' THEN 'Denmark'
					WHEN TRIM(UPPER(CNTRY)) LIKE 'GER%' THEN 'Germany'
					WHEN TRIM(UPPER(CNTRY)) LIKE 'CAN%' THEN 'Canada'
					WHEN TRIM(UPPER(CNTRY)) LIKE 'FRA%' THEN 'France'
					WHEN TRIM(UPPER(CNTRY)) LIKE 'AUS%' THEN 'Australia'
					ELSE 'Unknown'
					END AS CNTRY
		FROM bronze.erp_LOC_A101;

		TRUNCATE TABLE [silver].[erp_PX_CAT_G1V2] ;
		INSERT INTO [silver].[erp_PX_CAT_G1V2] (ID,CAT,SUBCAT,MAINTENANCE)
		SELECT TRIM(ID) AS ID,
			   TRIM(CAT) AS CAT,
			   TRIM(SUBCAT) AS SUBCAT,
			   TRIM(MAINTENANCE) AS MAINTENANCE
		FROM bronze.[erp_PX_CAT_G1V2];
		SET @endtime = GETDATE();
		PRINT 'Time taken to run operations is:' + CAST(ABS(DATEDIFF(second,@starttime,@endtime)) AS VARCHAR)
	END TRY
	BEGIN CATCH
	PRINT 'There is an error in one of the tables'
	END CATCH
END;

EXEC silver.load_silver;

