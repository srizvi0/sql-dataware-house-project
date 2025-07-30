/*
Define Tables for crm and erp source systems
*/

/* Logic to see if tables exist prior to creation */


IF OBJECT_ID ('bronze.crm_cust_info', 'U') is not null 
	DROP Table bronze.crm_cust_info; -- U is user defined

/* Create SRM & ERM Tables*/

Create TABLE bronze.crm_cust_info (
	cst_id int,
	cst_key varchar(50),
	cst_firstname varchar(50),
	cst_lastname varchar(50),
	cst_marital_status varchar(50),
	cst_gndr varchar(50),
	cst_create_date date
);
go

IF OBJECT_ID ('bronze.crm_prd_info', 'U') is not null
	DROP TABLE bronze.crm_prd_info

Create TABLE bronze.crm_prd_info(
	prd_id int,
	prd_key varchar(50),
	prd_nm varchar(50),
	prd_cost int,
	prd_line varchar(50),
	prd_start_dt date,
	prd_end_dt date
);

go

IF OBJECT_ID ('bronze.crm_sales_details', 'U') is not null
	DROP TABLE bronze.crm_sales_details

Create TABLE bronze.crm_sales_details(
	sls_ord_num varchar (50),
	sls_prd_key varchar (50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int
);

go

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') is not null
	DROP TABLE bronze.erp_cust_az12

Create table bronze.erp_cust_az12(
	CID varchar (50),
	BDATE date,
	GEN varchar (50)
)

go

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') is not null
	DROP TABLE bronze.erp_loc_a101

create table bronze.erp_loc_a101(
	CID varchar(50),
	CNTRY varchar (50)
)

go

IF OBJECT_ID ('bronze.erp_px_cat_G1V2', 'U') is not null
	DROP TABLE bronze.erp_px_cat_G1V2

create table bronze.erp_px_cat_G1V2(
	ID VARCHAR(15),
	CAT varchar (50),
	SUBCAT varchar (50),
	MAINTENANCE varchar (50)
)