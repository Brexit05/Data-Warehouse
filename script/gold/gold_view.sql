USE DataWarehouse;


CREATE VIEW gold.dim_customers AS
SELECT ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
	   c1.cst_id AS customer_id,
	   c1.cst_key AS customer_number,
	   c1.cst_firstname AS first_name,
	   c1.cst_lastname AS last_name,
	   c3.CNTRY AS country,
	   c1.cst_material_status AS marital_status,
	   CASE
			WHEN c1.cst_gndr != 'Unknown' THEN c1.cst_gndr -- CRM is the Master table for gender info
			ELSE ISNULL(c2.GEN, 'Unknown') 
			END AS gender,
	   c2.BDATE AS birth_date,
	   c1.cst_create_date AS create_date	   
FROM silver.crm_cust_info AS c1
LEFT JOIN silver.erp_CUST_AZ12 AS c2
ON c1.cst_key = c2.CID 
LEFT JOIN silver.erp_LOC_A101 AS c3
ON c1.cst_key = c3.CID

CREATE VIEW  gold.dim_products AS 
SELECT 
ROW_NUMBER() OVER(ORDER BY pn.prd_start_date, pn.prd_key) AS product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.CAT AS category,
pc.SUBCAT AS sub_category,
pc.MAINTENANCE AS maintenance,
pn.prd_cost AS cost,
pn.prd_line AS product_line,
pn.prd_start_date AS start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_PX_CAT_G1V2 AS pc
ON pn.cat_id = pc.ID
WHERE prd_end_date IS NULL;
--  We are making use of only the current in formation of the products

CREATE VIEW gold.fact_sales AS 
SELECT
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_id,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
ON sd.sls_cust_id = cu.customer_id;



































