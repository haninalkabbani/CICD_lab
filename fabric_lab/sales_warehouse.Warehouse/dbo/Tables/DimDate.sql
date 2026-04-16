CREATE TABLE [dbo].[DimDate] (

	[date_key] int NULL, 
	[date] date NULL, 
	[day] int NULL, 
	[month] int NULL, 
	[month_name] varchar(50) NULL, 
	[quarter] int NULL, 
	[year] int NULL, 
	[day_of_week] varchar(10) NULL, 
	[week_of_year] int NULL, 
	[is_weekend] bit NULL
);