-- Luke Calleja - ITSWD6.2A - Business Intelligence & Reporting Assignment
-- Cleanup
DROP TABLE [orderStar].[orderFact];
DROP TABLE [orderStar].[client];
DROP TABLE [orderStar].[date];
DROP TABLE [orderStar].[location];
DROP PROCEDURE [orderStar].[sp_addClient];
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
	, storeId UNIQUEIDENTIFIER NOT NULL
	, locationID UNIQUEIDENTIFIER NOT NULL
	, CONSTRAINT location_oltp_id UNIQUE(storeID)
	--, CONSTRAINT location_oltp_id UNIQUE(locationID)
);

CREATE TABLE [orderStar].[orderFact]
(
	orderKey UNIQUEIDENTIFIER  CONSTRAINT  order_fact_key PRIMARY KEY DEFAULT NEWID()
	, orderDate DATE NOT NULL
	--, orderTotal NUMERIC(6,2)
	, locKey UNIQUEIDENTIFIER NOT NULL CONSTRAINT order_LOCATION_key REFERENCES [orderStar].[location] (locationKey)
	, datKey UNIQUEIDENTIFIER NOT NULL CONSTRAINT order_TIME_key REFERENCES [orderStar].[date] (dateKey)
	, clieKey UNIQUEIDENTIFIER NOT NULL CONSTRAINT order_CLIENT_key REFERENCES [orderStar].[client] (clientKey)
	, ordID UNIQUEIDENTIFIER NOT NULL CONSTRAINT order_oltp_id UNIQUE
);
GO

-- ETL

-- Procedure for client entry/gathering
CREATE PROCEDURE [orderStar].[sp_addClient]
	(@firstName NVARCHAR(50), @lastName NVARCHAR(50), @country NVARCHAR(100), @region NVARCHAR(50), @city NVARCHAR(100), @gender CHAR(1), @age NUMERIC(3,0), @maritalStatus CHAR(1), @orderDate DATE, @clientId UNIQUEIDENTIFIER)
	AS
	BEGIN
		SET NOCOUNT ON;

		IF(NOT EXISTS (SELECT	clientKey
						FROM	[orderStar].[client]
						WHERE	country = @country AND region = @region AND city = @city AND gender = @gender
								AND age = @age AND maritialStatus = @maritalStatus AND clientID = @clientId))
		BEGIN
			UPDATE		[orderStar].[client]
				SET		toDate = DATEADD(DAY, -1, @orderDate)
				WHERE	clientID = @clientId AND toDate IS NULL;

			INSERT [orderStar].[client] 
				(firstName, lastName, country, region, city, gender, age, maritialStatus, fromDate, toDate, clientID)
			VALUES 
				(@firstName, @lastName, @country, @region, @city, @gender, @age, @maritalStatus, @orderDate, NULL, @clientId)
		END;
	END;
GO

BEGIN
	SET NOCOUNT ON;

	-- DATE
	INSERT INTO [orderStar].[date] (yearValue, quarterValue, monthValue, dayOfTheMonth, dayOfTheWeek, dateValue)
	(SELECT DISTINCT DATEPART(YEAR, orderDate), DATEPART(QUARTER, orderDate), DATEPART(MONTH, orderDate), DATEPART(DAY, orderDate),  DATEPART(WEEKDAY, orderDate), CAST(orderDate AS DATE) FROM [oltp].[order]);

	-- CLIENT
	DECLARE clientCursor CURSOR FOR
		SELECT		cli.firstName
					, cli.lastName
					, cou.countryName
					, reg.regionName
					, cty.cityName
					, cli.gender
					, DATEDIFF(year, cli.dateOfBirth, ord.orderDate) -- to get age of client				
					, cli.maritialStatus
					, ord.orderDate
					, cli.clientId
		FROM		[oltp].[client] cli
					JOIN
					[oltp].[city] cty
					ON
					cli.cityId = cty.cityId
					JOIN
					[oltp].[region] reg
					ON
					cty.regionId = reg.regionId
					JOIN
					[oltp].[country] cou
					ON
					reg.countryId = cou.countryId
					JOIN
					[oltp].[order] ord
					ON
					ord.clientId = cli.clientId
		ORDER BY	ord.orderDate

	DECLARE @firstName NVARCHAR(50);
	DECLARE @lastName NVARCHAR(50);
	DECLARE @country NVARCHAR(100);
	DECLARE @region NVARCHAR(50);
	DECLARE @city NVARCHAR(100);
	DECLARE @gender CHAR(1);
	DECLARE @age NUMERIC(3,0);
	DECLARE @maritalStatus CHAR(1);
	DECLARE @orderDate DATE;
	DECLARE @clientId UNIQUEIDENTIFIER;

	-- get the first client from the cursor
	OPEN clientCursor
		FETCH NEXT FROM clientCursor INTO @firstName, @lastName, @country, @region, @city, @gender, @age, @maritalStatus, @orderDate, @clientId;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		-- Execute the stored procedure for insertion
		EXEC [orderStar].[sp_addClient] @firstName, @lastName, @country, @region, @city, @gender, @age, @maritalStatus, @orderDate, @clientId

		-- move on to the next row
		FETCH NEXT FROM clientCursor INTO @firstName, @lastName, @country, @region, @city, @gender, @age, @maritalStatus, @orderDate, @clientId;
	END;

	CLOSE clientCursor;
	DEALLOCATE clientCursor;

	-- LOCATION
	INSERT INTO [orderStar].[location] (country, region, city, storeName, storeID, locationID)
	(
		SELECT	DISTINCT cou.countryName
				, reg.regionName
				, cty.cityName
				, sto.storeName
				, sto.storeID
				, cty.cityId
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
	INSERT INTO [orderStar].[orderFact] (orderDate, ordID, clieKey, locKey, datKey)
	(
		SELECT	DISTINCT ord.orderDate
				, ord.orderId -- order id
				, (SELECT clientKey FROM [orderStar].[client] WHERE clientID = ord.clientId AND toDate is NULL) -- client key
				, (SELECT locationKey FROM [orderStar].[location] WHERE storeId = ord.storeId)-- location key
				, (SELECT dateKey FROM [orderStar].[date] WHERE dateValue = CAST(ord.orderDate AS DATE)) -- date key
		FROM	[oltp].[order] ord
				JOIN
				[oltp].[orderItem] oit
				ON
				oit.orderId = ord.orderId				
	);
END;
GO

-- Basic reports from star
SELECT * FROM [orderStar].[date] ORDER BY yearValue;
SELECT * FROM [orderStar].[client];
SELECT * FROM [orderStar].[location];
SELECT * FROM [orderStar].[orderFact];