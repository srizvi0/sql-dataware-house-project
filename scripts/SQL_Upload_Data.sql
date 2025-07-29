/* Truncate + insert data into CRM & ERP tables */

Truncate table Bronze.crm_cust_info 

BULK INSERT	Bronze.crm_cust_info -- Bulk inserts Fast Data into tables
FROM 'C:\Users\Najam\OneDrive\Desktop\Data Engineer\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2, -- Tells sql that second rows is first row of data
	FIELDTERMINATOR = ',', -- Breaks data at comma
	TABLOCK
);
select * from Datawarehouse.Bronze.crm_cust_info;
go

Truncate table Bronze.crm_prd_info

Bulk insert Bronze.crm_prd_info
From 'C:\Users\Najam\OneDrive\Desktop\Data Engineer\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
With(
	Firstrow =2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
select * from Bronze.crm_prd_info
go

Truncate table Bronze.crm_sales_details

bulk insert Bronze.crm_sales_details
From 'C:\Users\Najam\OneDrive\Desktop\Data Engineer\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH(
	FIRSTROW =2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
select * from Bronze.crm_sales_details
go

Truncate table Bronze.erp_cust_az12

bulk insert Bronze.erp_cust_az12
From 'C:\Users\Najam\OneDrive\Desktop\Data Engineer\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
select * from Bronze.erp_cust_az12
go

Truncate table Bronze.erp_loc_a101

bulk insert Bronze.erp_loc_a101
FROM 'C:\Users\Najam\OneDrive\Desktop\Data Engineer\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH(
	FIRSTROW =2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
select * from Bronze.erp_loc_a101
go

Truncate table Bronze.erp_px_cat_G1V2

bulk insert Bronze.erp_px_cat_G1V2
FROM 'C:\Users\Najam\OneDrive\Desktop\Data Engineer\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
select * from Bronze.erp_px_cat_G1V2
go
