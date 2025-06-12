
# 🏗️ Data Warehouse Project: Sales Analytics

## 📁 Project Overview
This project involves designing and building a multi-layered data warehouse to support comprehensive sales analytics. The architecture follows a **Bronze → Silver → Gold** layer pattern to ensure clean, enriched, and business-ready data.
---
## 📚 Layers Description
### 🥉 Bronze Layer
- **Purpose**: Raw data ingestion from CRM and ERP systems.
- **Sources**:
  - `crm_cust_info`
  - `crm_prd_info`
  - `crm_sales_details`
  - `erp_CUST_AZ12`
  - `erp_LOC_A101`
  - `erp_PX_CAT_G1V2`
> This layer holds raw, untransformed data as extracted from source systems.
---
### 🥈 Silver Layer
- **Purpose**: Cleaned and integrated datasets.
- **Processes**:
  - Standardizing column names
  - Deduplicating records
  - Handling nulls and inconsistent values
  - Basic joins between CRM and ERP sources
> This layer prepares the data for dimensional modeling by resolving quality and structural issues.
---
### 🥇 Gold Layer
- **Purpose**: Business-friendly models for reporting and analytics.
#### Views Created:
- `gold.dim_customers`: Contains unique customer information, gender logic, and demographic details.
- `gold.dim_products`: Contains active product information, categorized with subcategories and maintenance status.
- `gold.fact_sales`: Fact table joining sales data with product and customer dimensions.
> These views are optimized for analytics and designed with star schema principles.
---
## 🔎 Data Exploration & Analysis
### Dimensions
- Countries of customers
- Product categories and subcategories
- Customer age analysis
### Measures
- Total sales, quantity, average price
- Total customers and orders
### Revenue Analysis
- Revenue by country, category, customer
- Top and bottom performing products
### Time Series
- Sales trends over months and years
- Running totals and moving averages
### Performance & Segmentation
- Year-over-year product performance
- Product cost segmentation
- Customer segmentation (VIP, Regular, New)
---
## 📈 Example Queries
```sql
-- Total revenue per category
SELECT d.category, SUM(f.sales) AS total_revenue
FROM gold.dim_products AS d
RIGHT JOIN gold.fact_sales AS f
  ON d.product_key = f.product_key
GROUP BY d.category
ORDER BY total_revenue DESC;
```
---
## 🧠 Learnings & Tools
- Applied dimensional modeling with star schema
- Used T-SQL for ETL and analysis
- Created reusable analytical views
- Performed segmentation and performance metrics
---
## 🔄 Future Improvements
- Automate ETL using SQL Server Agent or Airflow
- Introduce Slowly Changing Dimensions (SCDs)
- Add time dimension for more granular time-series analysis
- Visualize insights in Power BI or Tableau

