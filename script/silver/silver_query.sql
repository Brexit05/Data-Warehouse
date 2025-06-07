/*
This project involved cleaning and transforming raw customer, product, and sales data from the bronze layer into refined silver tables for analytics and reporting. Key transformations included standardizing text casing, trimming whitespace, and replacing null or blank values with placeholders like 'UNKNOWN'. Dates such as customer birthdates, product launch dates, and sales-related timestamps were properly cast and validated. Numerical fields like sales quantities and prices were corrected where inconsistencies were found, with recalculated sales figures ensuring accuracy. The result is a set of clean, reliable silver-layer tables that maintain referential integrity and are ready for downstream analysis or business intelligence use.
*/


USE DataWarehouse;
DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
prd_id INT,
cat_id NVARCHAR(50),
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost DECIMAL(10,2),
prd_line NVARCHAR(50),
prd_start_date DATE,
prd_end_date DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt NVARCHAR(50),
sls_ship_dt NVARCHAR(50),
sls_due_dt NVARCHAR(50),
sls_sales INT,
sls_quantity INT,
sls_price DECIMAL(10,2),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
 
DROP TABLE silver.erp_CUST_AZ12;
CREATE TABLE silver.erp_CUST_AZ12 (
CID NVARCHAR(50),
BDATE DATE,
GEN NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
 

DROP TABLE silver.erp_LOC_A101;
CREATE TABLE silver.erp_LOC_A101 (
CID NVARCHAR(50),
CNTRY NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
 
DROP TABLE silver.erp_PX_CAT_G1V2;
CREATE TABLE silver.erp_PX_CAT_G1V2 (
ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

PRINT '-- Detect and fix Quality issues on crm_cust_info table--'
-- Detect and fix Quality issues on crm_cust_info table

-- Check for nulls or duplicates in primary key
SELECT cst_id,COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) !=1 or cst_id IS NULL;

-- Create a query to clean this issue
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
SELECT cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_material_status,
cst_gndr,
cst_create_date
FROM no_duplicate
WHERE ranks = 1 AND cst_id IS NOT NULL;

-- Check for unwanted spaces(Trim)
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_material_status
FROM bronze.crm_cust_info
WHERE cst_material_status != TRIM(cst_material_status);

-- Fixing this, we copy the no_duplicate query and add the Trim there
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
SELECT cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_material_status,
cst_gndr,
cst_create_date
FROM no_duplicate
WHERE ranks = 1 AND cst_id IS NOT NULL;

-- Data Standardization and Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

-- Create a case where M is Male and F is Female and the rest is Unknown 

SELECT  *,
	CASE 
	WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
	WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
	ELSE 'Unknown'
	END
FROM bronze.crm_cust_info;
-- Do the same for the cst_aterial_status column

SELECT *,
	CASE
	WHEN UPPER(cst_material_status) = 'S' THEN 'Single'
	WHEN UPPER(cst_material_status) = 'M' THEN 'Married'
	ELSE 'Undecided'
	END
FROM bronze.crm_cust_info;

-- Incorporate this into our no_duplicate query

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

-- Then we insert the updated no_duplicate query into silver.crm_cust_info with the Truncate function above it
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

SELECT *
FROM silver.crm_cust_info;

-- Check everything out

SELECT cst_id,COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) !=1 or cst_id IS NULL;

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_material_status
FROM silver.crm_cust_info
WHERE cst_material_status != TRIM(cst_material_status);

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

-- Our queries have no results which is what we were going for!

PRINT '-- Detect and fix Quality issues on crm_prd_info table--'
-- Detect and fix Quality issues on crm_prd_info table

SELECT *
FROM bronze.crm_prd_info;

-- Check for Nulls and Duplicates in the crm_prd_info

SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) != 1 OR prd_id IS NULL;

-- Split the prd_key into 2 parts and replace '-' with '_'

SELECT prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
	SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_date,
	prd_end_date
FROM bronze.crm_prd_info;

-- Check for unwanted spaces in prd_nm
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for Nulls or Negative numbers in prd_cost

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- We replace the Null with 0 with the ISNULL function

SELECT prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
	SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost,0),
	prd_line,
	prd_start_date,
	prd_end_date
FROM bronze.crm_prd_info;

-- Data standardization and Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- We replace these strings as shown below
SELECT prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
	SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost,0),
	prd_line,
	CASE
		WHEN TRIM(UPPER(prd_line)) = 'M' THEN 'Mountain'
		WHEN TRIM(UPPER(prd_line)) = 'S' THEN 'Other Sales'
		WHEN TRIM(UPPER(prd_line)) = 'R' THEN 'Road'
		WHEN TRIM(UPPER(prd_line)) = 'T' THEN 'Touring'
		ELSE 'Unknown'
		END AS prd_line,
	prd_start_date,
	prd_end_date
FROM bronze.crm_prd_info;

-- Check if Start date is greater than end date
SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_date > prd_end_date;

-- Lots of errors in the dates, we'll have to create an entirely different prd_end_date

SELECT prd_key,
	prd_start_date,
	DATEADD(DAY,-1,LEAD(prd_start_date) OVER(PARTITION BY prd_key ORDER BY prd_start_date ASC)) AS end_date
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509','AC-HE-HL-U509-R');

-- We implement this logic
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

-- Insert into the silver crm_prd_info table
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

-- Let us look at the tables
SELECT *
FROM silver.crm_prd_info;

SELECT *
FROM silver.crm_cust_info;

-- Let us check the issues

SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) != 1 OR prd_id IS NULL;

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

SELECT *
FROM silver.crm_prd_info
WHERE prd_start_date > prd_end_date;

-- We have no results, which means our queries are right!
PRINT 'Data Transformation for crm_sales_details'

SELECT*
FROM bronze.crm_sales_details;

-- Convert the Date columns to Date type

SELECT sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CAST(CAST(NULLIF(sls_order_dt,'0') AS VARCHAR) AS DATE) AS sls_order_dt,
	CAST(CAST(NULLIF(sls_ship_dt,'0') AS VARCHAR) AS DATE) AS sls_ship_dt,
	CAST(CAST(NULLIF(sls_due_dt,'0') AS VARCHAR) AS DATE) AS sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details;

-- Check for invalid dates

SELECT*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check if Sales = quantiy * Price
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
	  OR sls_sales IS NULL OR sls_quantity IS NULL or sls_price IS NULL
	  OR sls_sales <= 0 OR sls_quantity <= 0 or sls_price <= 0
	  ORDER BY sls_quantity, sls_price;

-- Let us fix it
SELECT	CASE 
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


-- Combine the whole query and insert into the silver query
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


-- We move onto the next schema erp

SELECT*
FROM bronze.erp_CUST_AZ12;

-- We have to clean up the CID

SELECT 
	   CASE 
			WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
			ELSE CID
			END AS CID,
	   BDATE,
	   GEN
FROM bronze.erp_CUST_AZ12;

-- Let us take a look at the birth dates

SELECT BDATE
FROM bronze.erp_CUST_AZ12
WHERE BDATE > GETDATE() OR BDATE < '1920-01-01';

-- We fix this immediately
SELECT 
	   CASE 
			WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
			ELSE CID
			END AS CID,
	   CASE 
			WHEN BDATE > GETDATE() OR BDATE < '1920-01-01' THEN NULL
			ELSE BDATE
			END AS BDATE,
	   GEN
FROM bronze.erp_CUST_AZ12;

-- Consistency in the Gen column
SELECT DISTINCT GEN
FROM bronze.erp_CUST_AZ12;

-- We fix this
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

-- We insert it into the silver layer
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

SELECT *
FROM silver.erp_CUST_AZ12;

-- We want to clean the erp_LOC_A101 table

SELECT*
FROM [bronze].[erp_LOC_A101];

-- We want to get rid of the '-', so as to match the CID in the CUST_AZ12 table

SELECT CID,
TRIM(REPLACE(CID,'-','')) AS CID
FROM bronze.erp_LOC_A101;

-- We want to see if the country column is good

SELECT DISTINCT CNTRY
FROM bronze.erp_LOC_A101;

--Let us clean up the country column

	SELECT DISTINCT
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

-- We make changes to the table as shown below

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


-- Then we insert into our silver table
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

SELECT*
FROM silver.erp_LOC_A101;

-- Let us proof check the table

SELECT CID
FROM silver.erp_LOC_A101
WHERE CID != TRIM(REPLACE(CID,'-',''));

SELECT DISTINCT CNTRY
FROM silver.erp_LOC_A101;

-- We want to clean the erp_PX_CAT_GIV2 table

SELECT*
FROM bronze.[erp_PX_CAT_G1V2];

-- Let us clean up the ID column

SELECT ID
FROM bronze.[erp_PX_CAT_G1V2]
WHERE ID != UPPER(TRIM(ID));

--Let us look at items in the Category
SELECT DISTINCT CAT
FROM bronze.[erp_PX_CAT_G1V2]
WHERE CAT != UPPER(TRIM(CAT));

--Let us look at items in the Subcategory
SELECT DISTINCT SUBCAT
FROM bronze.[erp_PX_CAT_G1V2]
;


--Let us look at items in the Maintenance
SELECT DISTINCT MAINTENANCE
FROM bronze.[erp_PX_CAT_G1V2]
;
-- To be on the safe side well use...
SELECT TRIM(ID) AS ID,
	   TRIM(CAT) AS CAT,
	   TRIM(SUBCAT) AS SUBCAT,
	   TRIM(MAINTENANCE) AS MAINTENANCE
FROM bronze.[erp_PX_CAT_G1V2];

-- We insert into the silver.erp_PX_CAT_G1V2
TRUNCATE TABLE [silver].[erp_PX_CAT_G1V2] ;
INSERT INTO [silver].[erp_PX_CAT_G1V2] (ID,CAT,SUBCAT,MAINTENANCE)
SELECT TRIM(ID) AS ID,
	   TRIM(CAT) AS CAT,
	   TRIM(SUBCAT) AS SUBCAT,
	   TRIM(MAINTENANCE) AS MAINTENANCE
FROM bronze.[erp_PX_CAT_G1V2];

SELECT *
FROM silver.[erp_PX_CAT_G1V2];

