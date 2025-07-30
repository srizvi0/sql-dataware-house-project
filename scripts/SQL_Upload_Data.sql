/* Truncate + insert data into CRM & ERP tables */

CREATE OR ALTER PROCEDURE bronze.load_bronze AS -- Create stored procedure to save frequently used procedures in DB
begin
	DECLARE @start_time DATETIME, @end_time DATETIME,@sum_start_info varchar(30),
	@sum_start_prd_info varchar(30), @sum_start_sales_details varchar(30), 
	@start_erp_cust_az12 varchar(30), @sum_start_erp_loc_a101 varchar (30),
	@start_erp_px_cat_G1V2 varchar(30), @SUM_TOTAL_TIME varchar(30);
	
	-- Declaring variable
	BEGIN TRY
		PRINT '===============================================================';
		PRINT 'Loading Bronze Layer';-- Print 
		PRINT '===============================================================';
	
		PRINT '__________________';
		PRINT 'Loading CRM Tables';
		PRINT '__________________';

		Truncate table Bronze.crm_cust_info;
		SET @start_time = GETDATE();
		BULK INSERT	Bronze.crm_cust_info -- Bulk inserts Fast Data into tables
		FROM 'C:\Users\Najam\OneDrive\Desktop\Data Engineer\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- Tells sql that second rows is first row of data
			FIELDTERMINATOR = ',', -- Breaks data at comma
			TABLOCK
		);
		SET @end_time = GETDATE();
		Set @sum_start_info = cast (DATEDIFF (second,@start_time, @end_time) as nvarchar(30));
		PRINT 'Load Time: ' + @sum_start_info;

		Truncate table Bronze.crm_prd_info;
		SET @start_time = GETDATE();
		Bulk insert Bronze.crm_prd_info
		From 'C:\Users\Najam\OneDrive\Desktop\Data Engineer\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		With(
			Firstrow =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		Set @sum_start_prd_info = cast (DATEDIFF (second,@start_time, @end_time) as nvarchar(30));
		PRINT 'Load Time: ' + @sum_start_prd_info;


		Truncate table Bronze.crm_sales_details;
		SET @start_time = GETDATE();
		bulk insert Bronze.crm_sales_details
		From 'C:\Users\Najam\OneDrive\Desktop\Data Engineer\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		SET @sum_start_sales_details = cast(DATEDIFF (second,@start_time, @end_time) as nvarchar(30));
		PRINT 'Load Time: ' + @sum_start_sales_details;


		PRINT '__________________';
		PRINT 'Loading ERP Tables';
		PRINT '__________________';


		set @start_time = GETDATE();
		Truncate table Bronze.erp_cust_az12;
		bulk insert Bronze.erp_cust_az12
		From 'C:\Users\Najam\OneDrive\Desktop\Data Engineer\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		SET @start_erp_cust_az12 = cast (DATEDIFF (second,@start_time, @end_time) as varchar(30));
		PRINT 'Load Time: ' + @start_erp_cust_az12;


		set @start_time = GETDATE();
		Truncate table Bronze.erp_loc_a101;
		bulk insert Bronze.erp_loc_a101
		FROM 'C:\Users\Najam\OneDrive\Desktop\Data Engineer\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		SET @sum_start_erp_loc_a101 = cast (DATEDIFF (second,@start_time, @end_time) as varchar(30));
		PRINT 'Load Time: ' + @sum_start_erp_loc_a101;

		set @start_time = GETDATE();
		Truncate table Bronze.erp_px_cat_G1V2;
		bulk insert Bronze.erp_px_cat_G1V2
		FROM 'C:\Users\Najam\OneDrive\Desktop\Data Engineer\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		SET @start_erp_px_cat_G1V2 = cast (DATEDIFF (second,@start_time, @end_time) as varchar(30));
		PRINT 'Load Time: ' + @start_erp_px_cat_G1V2;

		SET @SUM_TOTAL_TIME = @sum_start_info + @sum_start_prd_info + @sum_start_sales_details + 
		@start_erp_cust_az12 + @sum_start_erp_loc_a101 + @start_erp_px_cat_G1V2;

		Print 'Total Duration ' + @SUM_TOTAL_TIME 'Seconds'

	END TRY -- sql runs TRY Block, and if it fails -> then runs CATCH block to handle error
	BEGIN CATCH
		PRINT 'Error occured during loading of bronze layer';
	END CATCH
END

Exec bronze.load_bronze -- Executing Stored Procedure