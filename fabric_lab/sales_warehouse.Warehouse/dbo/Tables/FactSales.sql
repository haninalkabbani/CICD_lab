CREATE TABLE [dbo].[FactSales] (

	[sales_order_number] varchar(50) NULL, 
	[line_number] int NULL, 
	[customer_key] bigint NULL, 
	[product_key] bigint NULL, 
	[date_key] int NULL, 
	[order_date] date NULL, 
	[quantity] int NULL, 
	[unit_price] decimal(18,4) NULL, 
	[tax_amount] decimal(18,4) NULL, 
	[sales_amount] decimal(18,4) NULL, 
	[tax_amount_line] decimal(18,4) NULL, 
	[total_amount] decimal(18,4) NULL, 
	[etl_loaded_at] datetime2(6) NULL
);