Create or alter procedure silver.load_silver AS
BEGIN
	begin try
	-- Cleaning table silver.crm_cust_info

	Print 'Truncating table silver.crm_cust_info'
	truncate table silver.crm_cust_info

	Print 'Inserting Data into silver.crm_cust_info'
	Insert into silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
	select 
		cst_id,
		cst_key,
		trim(cst_firstname) as [cst_firstname],
		trim(cst_lastname) as [cst_lastname],
		case 
			 when UPPER(trim(cst_marital_status))= 'M' then 'Married'
			 when UPPER(trim(cst_marital_status))= 'S' then 'Single'
			 else 'N/A'
		end as [cst_marital_status],
		case
			when UPPER(cst_gndr)= 'M' then 'Male'
			when UPPER(cst_gndr)= 'F' then 'Female'
			else 'N/A'
		end as [cst_gndr],
		cst_create_date
	from(
		SELECT 
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date,
			rank() over (partition by cst_id order by cst_create_date desc) as [Ranked per date]
		FROM Bronze.crm_cust_info
	)t 
	where [Ranked per date] = 1;


	-- Cleaning crm_prd_info table
	-- Step 1: check duplicate (No duplicates found)
	-- Step 2: split the product key

	print 'truncating table silver.crm_prd_info'
	truncate table silver.crm_prd_info

	print 'inserting data into silver.crm_prd_info'
	insert into silver.crm_prd_info(prd_id, prd_key, cat_id, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
		select
			prd_id,
			SUBSTRING (prd_key,7,len(prd_key)) as [prd_key],
			Replace(SUBSTRING (prd_key,0, 6), '-', '_') as [cat_id],
			trim(prd_nm) as prd_nm,
			coalesce (prd_cost, 0) as prd_cost,
			case prd_line
				when 'M' then 'Mountain'
				when 'R' then 'Road'
				when 'S' then 'Other Sales'
				when 'T' then 'Touring'
				else 'N/A'
			end as [prd_line],
			prd_start_dt,
			DATEADD (DAY, -1 , LEAD(prd_start_dt) over (partition by prd_nm order by prd_start_dt)) as [prd_end_dt]
		from bronze.crm_prd_info
	go;

	-- clean and load crm_sales_details table

	print 'truncating table silver.crm_sales_details'
	truncate table silver.crm_sales_details

	print 'Inserting data into silver.crm_sales_details'
	insert into silver.crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
	select 
		trim (sls_ord_num) as sls_ord_num,
		trim (sls_prd_key) as sls_prd_key,
		sls_cust_id,
		case when 
		len(sls_order_dt) < 8 then null
		else format(cast (cast (sls_order_dt as varchar) as DATE), 'yyyy-MM-dd') 
		end as sls_order_dt,
		format(cast (cast (sls_ship_dt as varchar) as DATE), 'yyyy-MM-dd') as sls_ship_dt,
		format(cast (cast (sls_due_dt as varchar) as DATE), 'yyyy-MM-dd') as sls_due_dt,
		cast (isnull(abs(sls_sales),0) as numeric) as sls_sales, 
		cast (abs(sls_quantity) as numeric) as sls_quantity,
		cast (isnull(abs(sls_sales),0) as numeric) * cast (abs(sls_quantity) as numeric) as sls_price
	from bronze.crm_sales_details
	go;


	-- clean and load erp_cust_az12 table

	print 'truncating table silver.erp_cust_az12'
	truncate table silver.erp_cust_az12

	print 'Inserting data into silver.erp_cust_az12'
	Insert into silver.erp_cust_az12(CID, BDATE, GEN)
	select 
		CASE WHEN 
			LEN (CID) = 13 then SUBSTRING (CID, 4, len (CID))
			else CID
		end as CID,
		case 
			when BDATE > GETDATE() then NULL
			When BDATE < '1924-01-01' then NULL
			else BDATE
			end as BDATE,
		case GEN
			When 'F' then 'Female'
			when 'M' then 'Male'
			when ' ' then Null
			else GEN
		end as GEN
	from bronze.erp_cust_az12
	go;

	-- Clean & Load erp_loc_a101 table

	print 'Truncating table silver.erp_loc_a101'
	truncate table silver.erp_loc_a101

	print 'Inserting data into silver.erp_loc_a101'
	Insert into silver.erp_loc_a101(CID, CNTRY)
	select 
		REPLACE(CID, '-', '') as CID,
		Case CNTRY
			when 'US' then 'United States'
			when '' then 'n/a'
			when 'USA' then 'United States'
			when 'DE' then 'Germany'
			else CNTRY
		end as CNTRY
	from bronze.erp_loc_a101
	go;

	-- Clean & load erp_px_cat_g1v2 table

	print 'Trucating table silver.erp_px_cat_G1V2'
	truncate table silver.erp_px_cat_G1V2

	print 'Inserting data into silver.erp_px_cat_G1V2'
	Insert into silver.erp_px_cat_G1V2 (ID, CAT, SUBCAT, MAINTENANCE)
	select
		ID,
		Trim (CAT) AS CAT,
		SUBCAT,
		MAINTENANCE
	from bronze.erp_px_cat_G1V2
	go;

	end try

	begin catch
		print 'Unable to insert data to silver table'
	end catch
end
