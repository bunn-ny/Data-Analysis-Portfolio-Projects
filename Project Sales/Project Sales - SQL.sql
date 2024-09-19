/*
Skills used: User Defined Functions(UDFs), conditional logics, Table and data engineering, #temporary tables, window functions, joins,
	     Common Table Expressions(CTEs), column aggregations, date and time functions. 
*/
USE Sales;

DROP TABLE IF EXISTS sales;
CREATE TABLE sales(
	order_id INT,
	[product] NVARCHAR(200),
	quantity INT,
	price_each DECIMAL(10, 2),
	revenue DECIMAL (10, 2),
	order_date DATETIME,
	[Address] NVARCHAR(200));

DROP TABLE IF EXISTS #temp_sales;
WITH t1 AS(
	SELECT * FROM January
	UNION ALL
	SELECT * FROM February
	UNION ALL
	SELECT * FROM March
	UNION ALL
	SELECT * FROM April
	UNION ALL
	SELECT * FROM May
	UNION ALL
	SELECT * FROM June
	UNION ALL
	SELECT * FROM July
	UNION ALL
	SELECT * FROM August
	UNION ALL
	SELECT * FROM September
	UNION ALL
	SELECT * FROM October
	UNION ALL
	SELECT * FROM November
	UNION ALL
	SELECT * FROM December)
SELECT *
INTO #temp_sales
FROM T1;

-- removing blank rows and unwanted columns
DELETE FROM #temp_sales
WHERE ISNUMERIC([Order ID]) = 0;

--Deduplication
WITH T1 AS (
	SELECT *,
			ROW_NUMBER() OVER (
				PARTITION BY [Order ID],
							 [Product],
							 [Quantity Ordered],
							 [Price Each],
							 [Order Date],
							 [Purchase Address]
				ORDER BY [ORDER ID]) dups
	FROM #temp_sales)
DELETE FROM T1
WHERE DUPS > 1;

-- populate the sales table
INSERT INTO SALES(
				[order_id],
				[product],
				quantity,
				price_each,
				order_date,
				[address])
SELECT
	[order id],
	[product],
	[Quantity Ordered],
	[price each],
	[order date],
	[purchase address]
FROM #Temp_Sales;

-- generate the revenue column
DROP FUNCTION IF EXISTS CalculateRevenue;

CREATE FUNCTION CalculateRevenue(@quantity INT, @price DECIMAL(10, 2))
RETURNS DECIMAL(10, 2)
AS
BEGIN
    RETURN @quantity * @price
END;

UPDATE sales
SET revenue = dbo.CalculateRevenue(quantity, price_each);

-- EDA
-- REVENUE BY CITY
WITH T1 AS(
	SELECT 
		*,
		REPLACE([Address], '"', '') AS refined_address
	FROM sales),
T2 AS (
	SELECT
		revenue,
		LEFT(refined_address, CHARINDEX(',', refined_address) - 1) AS [addresses],
		TRIM(SUBSTRING(
			refined_address,
			CHARINDEX(',', refined_address) + 1,
			CHARINDEX(',', refined_address, CHARINDEX(',', refined_address) + 1) - CHARINDEX(',', refined_address) - 1
		)) AS cities,
	   TRIM(SUBSTRING(
			refined_address,
			CHARINDEX(',', refined_address, CHARINDEX(',', refined_address) + 1) + 1,
			LEN(refined_address) - CHARINDEX(',', refined_address, CHARINDEX(',', refined_address) + 1)
		)) AS state_zips
	FROM t1)
SELECT cities, SUM(revenue) city_revenue
FROM t2
GROUP BY cities
ORDER BY city_revenue DESC

-- revenue by month
WITH T1 AS(
	SELECT
		MONTH(order_date) as month_number,
		DATENAME(MONTH, Order_Date) as [month], 
		SUM(revenue) OVER (PARTITION BY MONTH(order_date) ORDER BY order_date)as run_total_revenue
	FROM sales)
SELECT [month],
	   MAX(run_total_revenue) AS revenue_by_month
FROM T1
GROUP BY [month]
ORDER BY revenue_by_month DESC;

-- sales by the hour
SELECT DATEPART(HOUR, order_date) hour_of_order, SUM(revenue)as revenue_by_time
FROM sales
GROUP BY DATEPART(HOUR, order_date)
ORDER BY revenue_by_time;

-- revenue by time_of_day
WITH T1 AS(
SELECT *,
	CASE 
		WHEN CONVERT(time, order_date) BETWEEN '00:00:00' AND '12:00:00' THEN 'Morning'
		WHEN CONVERT(time, order_date) BETWEEN '12:01:00' AND '16:00:00' THEN 'Afternoon'
		ELSE 'Evening'
	END AS time_of_day
FROM sales)
SELECT Time_of_Day, SUM(REVENUE) as sum_revenue
FROM T1
GROUP BY time_of_day
ORDER BY sum_revenue DESC;

-- number of products sold
SELECT [product], COUNT([product]) AS products_sold
FROM sales
GROUP BY [product]
ORDER BY products_sold DESC;

-- basket analysis
SELECT
    a.[product] AS product_A,
    b.[product] AS product_B,
    COUNT(*) AS frequency
FROM sales a
JOIN sales b
	ON a.order_id = b.order_id 
	AND a.[product] < b.[product]
GROUP BY a.[product], b.[product]
ORDER BY frequency DESC;
