/*This script defines the structure for Silver Layer tables in your data warehouse. These tables serve as cleaned and slightly enriched versions of the raw data from the Bronze Layer.*/

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
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
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

