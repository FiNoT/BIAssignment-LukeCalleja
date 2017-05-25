-- Luke Calleja - ITSWD6.2A - Business Intelligence & Reporting Assignment
-- Cleanup
DROP TABLE [orderStar].[orderFact];
DROP TABLE [orderStar].[client];
DROP TABLE [orderStar].[date];
DROP TABLE [orderStar].[location];
DROP SCHEMA [orderStar];
GO

-- basic selects
USE [iict6011a02];
GO

-- Selects
select * from [oltp].[city]
select * from [oltp].[region]
select * from [oltp].[country]
select * from [oltp].[client] WHERE cityId = 'ADBC6B9A-73BC-465A-9ECB-D612F9C8785B' ORDER BY [dateOfBirth] -- 8736 total rows
select * from [oltp].[order] ORDER BY [orderDate] -- 54600 total rows
select * from [oltp].[store]
select * from [oltp].[storeType]
select * from [oltp].[brand]
select * from [oltp].[orderItem]
select * from [oltp].[client] ORDER BY dateOfBirth
select * from [oltp].[yearlyIncome]
select * from [oltp].[educationLevel]
select * from [oltp].[membershipLevel]

-- Checking that data is correct. Total number of clients is 8736
SELECT  *
FROM	[oltp].[client]
WHERE	gender = 'M'
		OR gender = 'F'

SELECT	*
FROM	[oltp].[client]
WHERE	maritialStatus = 'M'
		OR maritialStatus = 'S'

SELECT	*
FROM	[oltp].[client]
WHERE	dateOfBirth > '1900-01-01'
		AND dateOfBirth < '1990-12-12'

-- Total number of orders is 54600
SELECT	*
FROM	[oltp].[order]
WHERE	orderDate >= '1997-01-01'
		AND orderDate <= '1998-12-31'

-- OLAP Cube (TBC)
SELECT	*
FROM	[oltp].[order]
WHERE	storeId IN (SELECT	storeId
					FROM	[oltp].[store]
					WHERE	cityId IN (SELECT	cityId
										FROM	[oltp].[city]
										WHERE	regionId IN (SELECT	regionId
															FROM	[oltp].[region]
															WHERE	regionName = 'Guerrero')));

-- Star Schema Creation
CREATE SCHEMA [orderStar];
GO

CREATE TABLE [orderStar].[client]
(
	clientKey UNIQUEIDENTIFIER CONSTRAINT client_dim_key PRIMARY KEY DEFAULT NEWID()
	, firstName NVARCHAR(50) NOT NULL
	, lastName NVARCHAR(50) NOT NULL
	, country NVARCHAR(100) NOT NULL
	, region NVARCHAR(50) NOT NULL
	, city NVARCHAR(100) NOT NULL
	, gender CHAR(1) NOT NULL
	, age NUMERIC(3,0) NOT NULL
	, fromDate DATE NOT NULL
	, toDate DATE DEFAULT NULL
	, maritialStatus CHAR(1) NOT NULL
	, clientID UNIQUEIDENTIFIER NOT NULL
	, CONSTRAINT client_nat_key UNIQUE(fromDate)
	, CONSTRAINT client_oltp_id UNIQUE(clientID, fromDate)
);

CREATE TABLE [orderStar].[date]
(
	dateKey UNIQUEIDENTIFIER CONSTRAINT date_dim_key PRIMARY KEY DEFAULT NEWID()
	, yearValue NUMERIC(4,0) NOT NULL
	, quarterValue NUMERIC(1,0) NOT NULL
	, monthValue NUMERIC(2,0) NOT NULL
	, dayOfTheMonth NUMERIC(2,0) NOT NULL
	, dayOfTheWeek NUMERIC(1,0) NOT NULL
	, dateValue DATE CONSTRAINT date_nat_key UNIQUE NOT NULL
);

CREATE TABLE [orderStar].[location]
(
	locationKey UNIQUEIDENTIFIER CONSTRAINT location_dim_key PRIMARY KEY DEFAULT NEWID()
	, country NVARCHAR(100) NOT NULL
	, region NVARCHAR(50) NOT NULL
	, city NVARCHAR(100) NOT NULL
	, storeName NVARCHAR(100) NOT NULL
	, locationID UNIQUEIDENTIFIER NOT NULL
	, CONSTRAINT location_nat_key UNIQUE(storeName)
	, CONSTRAINT location_oltp_id UNIQUE(locationID)
);

CREATE TABLE [orderStar].[orderFact]
(
	orderKey UNIQUEIDENTIFIER  CONSTRAINT  order_fact_key PRIMARY KEY DEFAULT NEWID()
	, orderDate DATE NOT NULL
	, orderTotal NUMERIC(6,2)
	, locKey UNIQUEIDENTIFIER NOT NULL CONSTRAINT order_LOCATION_key REFERENCES [orderStar].[location] (locationKey)
	, datKey UNIQUEIDENTIFIER NOT NULL CONSTRAINT order_TIME_key REFERENCES [orderStar].[date] (dateKey)
	, clieKey UNIQUEIDENTIFIER NOT NULL CONSTRAINT order_CLIENT_key REFERENCES [orderStar].[client] (clientKey)
	, ordID UNIQUEIDENTIFIER NOT NULL CONSTRAINT order_otp_id UNIQUE
);

-- ETL
BEGIN
	SET NOCOUNT ON;

	-- DATE
	INSERT INTO [orderStar].[date] (yearValue, quarterValue, monthValue, dayOfTheMonth, dayOfTheWeek, dateValue)
	(SELECT DISTINCT DATEPART(YEAR, orderDate), DATEPART(QUARTER, orderDate), DATEPART(MONTH, orderDate), DATEPART(DAY, orderDate),  DATEPART(WEEKDAY, orderDate), CAST(orderDate AS DATE) FROM [oltp].[order]);

	-- CLIENT
	INSERT INTO [orderStar].[client] (firstName, lastName, country, region, city, gender, age, fromDate, toDate, maritialStatus, clientID)
	(

	);

	-- LOCATION
	INSERT INTO [orderStar].[location] (country, region, city, storeName, locationID)
	(
		SELECT	DISTINCT cou.countryName
				, reg.regionName
				, cty.cityName
				, sto.storeName
				, reg.regionId
		FROM	[oltp].[store] sto
				JOIN
				[oltp].[city] cty
				ON
				sto.cityId = cty.cityId
				JOIN
				[oltp].[region] reg
				ON
				cty.regionId = reg.regionId
				JOIN
				[oltp].[country] cou
				ON
				reg.countryId = cou.countryId
	);

	-- ORDER FACT
	INSERT INTO [orderStar].[orderFact] (orderDate, orderTotal, ordID, clieKey, locKey, datKey)
	(
		SELECT	ord.orderDate
				, 
				, (SELECT ) -- order id
				, (SELECT clientKey FROM [orderStar].[client] WHERE clientID = ord.clientId AND toDate is NULL) -- client key
				, (SELECT ) -- location key
				, (SELECT dateKey FROM [orderStar].[date] WHERE dateValue = CAST(ord.orderDate)) -- date key
		FROM	[oltp].[order] ord
				JOIN
				[oltp].[orderItem] oit
				ON
				oit.orderId = ord.orderId
				
	);
END;
GO

SELECT * FROM [orderStar].[date] ORDER BY yearValue;
SELECT * FROM [orderStar].[client];
SELECT * FROM [orderStar].[location];
SELECT * FROM [orderStar].[orderFact];