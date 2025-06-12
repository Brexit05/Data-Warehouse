/*This stored procedure is responsible for automating the loading of raw data into the Bronze Layer of your Data Warehouse using BULK INSERT.*/

 -- Creating stored procedure 
 CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
 BEGIN
	DECLARE @start DATETIME,@end DATETIME
	BEGIN TRY
		PRINT '====================';
		PRINT 'Loading Bronze Layer';
		PRINT '====================';
		 -- Loading the Tables using Bulk insert
		PRINT '---------------------';
		PRINT   'On the crm folder';
		PRINT '---------------------';

		PRINT '>> Truncating table: bronze.crm_cust_info'
	SET @start = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT '>> Inserting Data into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\HP\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

	
		PRINT '>> Truncating table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT '>> Inserting Data into: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\HP\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK);


		PRINT '>> Truncating table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT '>> Inserting Data into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\HP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR =  ',',
		TABLOCK);


		PRINT '---------------------';
		PRINT   'On the erp folder';
		PRINT '---------------------';

		PRINT '>> Truncating table: bronze.erp_CUST_AZ12'
		TRUNCATE TABLE bronze.erp_CUST_AZ12
		PRINT '>> Inserting Data into: bronze.erp_CUST_AZ12'
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\HP\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK);


		PRINT '>> Truncating table: bronze.erp_LOC_A101'
		TRUNCATE TABLE bronze.erp_LOC_A101
		PRINT '>> Inserting Data into: bronze.erp_LOC_A101'
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\HP\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
		FIRSTROW = 1,
		FIELDTERMINATOR = ',',
		TABLOCK);


		PRINT '>> Truncating table: bronze.erp_PX_CAT_G1V2'
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2
		PRINT '>> Inserting Data into: bronze.erp_PX_CAT_G1V2'
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\HP\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK);
	SET @end = GETDATE();
	PRINT 'Bronze Stage took :' + CAST(DATEDIFF(second, @start,@end) AS NVARCHAR) + ' seconds';

	END TRY
	BEGIN CATCH
	PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
	PRINT      'There is an error.'
	END CATCH
END;

EXEC bronze.load_bronze;

