/*
This script initializes the bronze layer of a data warehouse by first creating the DataWarehouse database along with its corresponding bronze, silver, and gold schemas, which represent the raw, cleansed, and analytics-ready layers respectively. Within the bronze schema, it creates six foundational tables to ingest raw data from various source systems: crm_cust_info, crm_prd_info, crm_sales_details, erp_CUST_AZ12, erp_LOC_A101, and erp_PX_CAT_G1V2. These tables are designed to capture unprocessed data, preserving the original structure and formats—including raw strings for dates and loosely structured categorical fields—ensuring no rows are lost during ingestion. This layer serves as the landing zone for all data before transformation begins in the silver layer.
*/

--Building the Bronze Layer(Loading the data from the source into the Data warehouse and making sure no rows are missing.)
-- Creating Database and Schemas
CREATE DATABASE DataWarehouse;
USE DataWarehouse;

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
-- Creating Tables 

CREATE TABLE bronze.crm_cust_info (
 cst_id INT,
 cst_key NVARCHAR(50),
 cst_firstname NVARCHAR(50),
 cst_lastname NVARCHAR(50),
 cst_material_status NVARCHAR(50),
 cst_gndr NVARCHAR(50),
 cst_create_date DATE
 );
 
 CREATE TABLE bronze.crm_prd_info (
 prd_id INT,
 prd_key NVARCHAR(50),
 prd_nm NVARCHAR(50),
 prd_cost DECIMAL(10,2),
 prd_line NVARCHAR(50),
 prd_start_date DATE,
 prd_end_date DATE
 );

 CREATE TABLE bronze.crm_sales_details (
 sls_ord_num NVARCHAR(50),
 sls_prd_key NVARCHAR(50),
 sls_cust_id INT,
 sls_order_dt NVARCHAR(50),
 sls_ship_dt NVARCHAR(50),
 sls_due_dt NVARCHAR(50),
 sls_sales INT,
 sls_quantity INT,
 sls_price DECIMAL(10,2)
 );
 
 CREATE TABLE bronze.erp_CUST_AZ12 (
 CID NVARCHAR(50),
 BDATE DATE,
 GEN NVARCHAR(50)
 );
 
 CREATE TABLE bronze.erp_LOC_A101 (
 CID NVARCHAR(50),
 CNTRY NVARCHAR(50)
 );
 
 CREATE TABLE bronze.erp_PX_CAT_G1V2 (
 ID NVARCHAR(50),
 CAT NVARCHAR(50),
 SUBCAT NVARCHAR(50),
 MAINTENANCE NVARCHAR(50)
 );
