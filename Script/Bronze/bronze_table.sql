/*This SQL script sets up the Bronze Layer of a Data Warehouse in SQL Server. The Bronze Layer is the raw ingestion layer, where data is first loaded from source systems before being transformed in later layers (Silver and Gold). The primary goal of this layer is to preserve raw data exactly as it comes from the source, ensuring nothing is lost.*/

-- Building the Bronze Layer(Loading the data from the source into the Data warehouse and making sure no rows are missing.)
-- Creating Database and Schemas
CREATE DATABASE DataWarehouse;
USE DataWarehouse;

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
-- Creating Tables 

DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
 cst_id INT,
 cst_key NVARCHAR(50),
 cst_firstname NVARCHAR(50),
 cst_lastname NVARCHAR(50),
 cst_material_status NVARCHAR(50),
 cst_gndr NVARCHAR(50),
 cst_create_date DATE
 );
 
 DROP TABLE bronze.crm_prd_info;
 CREATE TABLE bronze.crm_prd_info (
 prd_id INT,
 prd_key NVARCHAR(50),
 prd_nm NVARCHAR(50),
 prd_cost DECIMAL(10,2),
 prd_line NVARCHAR(50),
 prd_start_date DATE,
 prd_end_date DATE
 );

 DROP TABLE bronze.crm_sales_details;
 CREATE TABLE bronze.crm_sales_details (
 sls_ord_num NVARCHAR(50),
 sls_prd_key NVARCHAR(50),
 sls_cust_id INT,
 sls_order_dt INT,
 sls_ship_dt INT,
 sls_due_dt INT,
 sls_sales INT,
 sls_quantity INT,
 sls_price DECIMAL(10,2)
 );
 
 DROP TABLE bronze.erp_CUST_AZ12;
 CREATE TABLE bronze.erp_CUST_AZ12 (
 CID NVARCHAR(50),
 BDATE DATE,
 GEN NVARCHAR(50)
 );
 
 DROP TABLE bronze.erp_LOC_A101;
 CREATE TABLE bronze.erp_LOC_A101 (
 CID NVARCHAR(50),
 CNTRY NVARCHAR(50)
 );
 
 DROP TABLE bronze.erp_PX_CAT_G1V2;
 CREATE TABLE bronze.erp_PX_CAT_G1V2 (
 ID NVARCHAR(50),
 CAT NVARCHAR(50),
 SUBCAT NVARCHAR(50),
 MAINTENANCE NVARCHAR(50)
 );


