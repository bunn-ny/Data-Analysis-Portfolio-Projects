/*
Skills used: Sub queries, views,  Common Table Expressions(CTEs), window functions, joins,
			 columns aggregations, advanced analytical functions, table partitioning,
			 data cleaning and feature engineering, conditional logics, type conversions
*/
USE [Project Covid];
----------------------------------1. DATA CLEANING & MANIPULATION----------------------------------
-- Select only the required columns into a table, Deaths.
DROP TABLE IF EXISTS deaths
CREATE TABLE deaths(
	[date] DATE,
	continent VARCHAR(50),
	[location] VARCHAR(100),
	[population] NUMERIC,
	new_cases INT,
	total_cases INT,
	new_deaths INT,
	total_deaths INT);

TRUNCATE TABLE deaths;
INSERT INTO deaths(
SELECT
	[date],
	continent,
	[location],
	[population], 
	new_cases, 
	total_cases, 
	new_deaths, 
	total_deaths
FROM CovidDeaths);

-- As the data is uploaded weekly (every sunday), delete records from other days.
DELETE FROM Deaths
WHERE date IN (
    SELECT [date]
    FROM Deaths
    WHERE DATENAME(WEEKDAY, [date]) != 'Sunday');

-- Deduplication.
WITH t1 AS (
    SELECT *,
           ROW_NUMBER() OVER (
				PARTITION BY location, [date]
				ORDER BY (SELECT NULL)
			) AS duplicates
    FROM Deaths)
DELETE FROM t1
WHERE duplicates > 1;

----------------------------------HANDLING NULLS----------------------------
-- Nulls to 0.
UPDATE Deaths
SET 
    total_cases = ISNULL(total_cases, 0),
    new_cases = ISNULL(new_cases, 0),
    total_deaths = COALESCE(total_deaths, 0),
    new_deaths = COALESCE(new_deaths, 0);

-- Populate total cases where missing.
WITH t1 AS (
    SELECT
        date,
        continent,
        location,
        new_cases,
        total_cases,
        LEAD(total_cases) OVER (PARTITION BY location ORDER BY date) - LEAD(new_cases) OVER (PARTITION BY location ORDER BY date) AS total_cases_raw,
        CASE
            WHEN LEAD(total_cases) OVER (PARTITION BY location ORDER BY date) - LEAD(new_cases) OVER (PARTITION BY location ORDER BY date) = total_cases THEN 'True'
            ELSE 'False'
        END AS match_total_cases
    FROM Deaths)
UPDATE t1
SET total_cases = total_Cases_raw
WHERE total_cases_raw > 0 and match_total_cases = 'False';

--Derive new cases from known total cases
WITH t1 AS (
    SELECT 
        date, 
        continent, 
        location, 
        total_cases, 
        new_cases,
        total_cases - LAG(total_cases) OVER (PARTITION BY location ORDER BY date) AS new_cases_raw,
        LAG(new_cases) OVER (PARTITION BY location ORDER BY date) AS lag_new_cases,
        CASE
            WHEN total_cases - LAG(total_cases) OVER (PARTITION BY location ORDER BY date) = new_cases THEN 'True'
            ELSE 'False'
        END AS match_new_cases
    FROM Deaths)
UPDATE d
SET d.new_cases = t1.new_cases_raw
FROM Deaths d
INNER JOIN t1 ON d.date = t1.date
              AND d.location = t1.location
WHERE t1.new_cases_raw > 0 
  AND t1.match_new_cases = 'False' 
  AND t1.lag_new_cases != 0;

--Populate total deaths where missing
WITH t1 AS (
    SELECT
        date,
        continent,
        location,
        new_deaths,
        total_deaths,
        LEAD(total_deaths) OVER (PARTITION BY location ORDER BY date) - LEAD(new_deaths) OVER (PARTITION BY location ORDER BY date) AS total_deaths_raw,
        CASE
            WHEN LEAD(total_deaths) OVER (PARTITION BY location ORDER BY date) - LEAD(new_deaths) OVER (PARTITION BY location ORDER BY date) = total_deaths THEN 'True'
            ELSE 'False'
        END AS match_total_deaths
    FROM Deaths)
UPDATE t1
SET total_deaths = total_deaths_raw
WHERE total_deaths_raw <> 0 and match_total_deaths = 'False';

-- Derive new deaths from known total deaths
WITH t1 AS (
    SELECT 
        date, 
        continent, 
        location, 
        total_deaths, 
        new_deaths,
        total_deaths - LAG(total_deaths) OVER (PARTITION BY location ORDER BY date) AS new_deaths_raw,
        LAG(new_deaths) OVER (PARTITION BY location ORDER BY date) AS lag_new_deaths,
        CASE
            WHEN total_deaths - LAG(total_deaths) OVER (PARTITION BY location ORDER BY date) = new_deaths THEN 'True'
            ELSE 'False'
        END AS match_new_deaths
    FROM Deaths)
UPDATE d
SET d.new_deaths = t1.new_deaths_raw
FROM Deaths d
INNER JOIN t1 ON d.[date] = t1.[date]
              AND d.[location] = t1.[location]
WHERE t1.new_deaths_raw > 0 
  AND t1.match_new_deaths = 'False' 
  AND t1.lag_new_deaths != 0;

--------------------------------2. EXPLORATORY DATA ANALYSIS (EDA)---------------------------------
--------------Views have been created to store some of these for later visualisations--------------
-- ANALYSIS BY COUNTRY.
-- Total cases vs population
DROP VIEW IF EXISTS population_infected;

CREATE VIEW population_infected as (
SELECT
	[location],
	[population],
	MAX(CAST(total_cases AS FLOAT)) cumulative_cases,
	ROUND((MAX(CAST(total_cases AS FLOAT))/[population]) * 100, 2) percentage_cases
FROM Deaths
WHERE continent IS NOT NULL
GROUP BY [location], [population]
HAVING MAX(CAST(total_cases AS FLOAT)) != 0);

-- Total deaths vs population
DROP VIEW IF EXISTS population_fatality;

CREATE VIEW population_fatality as (
SELECT
	[location],
	[population],
	MAX(CONVERT(FLOAT, total_deaths)) cumulative_deaths,
	ROUND((MAX(CONVERT(FLOAT, total_deaths))/[population]) * 100, 2) percentage_deaths
FROM Deaths
WHERE continent IS NOT NULL
GROUP BY [location], [population]
HAVING MAX(CONVERT(FLOAT, total_deaths)) <> 0);

-- Case fatality rate
DROP VIEW IF EXISTS case_fatality;

CREATE VIEW case_fatality as (
SELECT
	[location],
	MAX(total_cases) cumulative_cases,
	MAX(total_deaths) cumulative_deaths,
	ROUND((MAX(total_deaths)/MAX(total_cases)) * 100, 2) fatality_rate
FROM Deaths
WHERE continent IS NOT NULL AND total_cases <> 0
GROUP BY [location]
ORDER BY fatality_rate DESC);

-- Average daily new cases
SELECT [location], ROUND(AVG(new_cases), 2) AS avg_daily_new_cases
FROM Deaths
WHERE continent IS NOT NULL AND new_cases IS NOT NULL
GROUP BY [location]
ORDER BY avg_daily_new_cases DESC;

-- GLOBAL ANALYSIS
-- Death's by continent
WITH t1 as
	(SELECT
		[location],
		continent,
		[population], 
		MAX(total_deaths) AS deaths_per_country
	FROM Deaths
	WHERE continent IS NOT NULL
	GROUP BY
		[location], 
		continent,
		[population])
SELECT
	continent,
	SUM([population]) [continent's_population],
	SUM(deaths_per_country) deaths_by_continent,
	ROUND((SUM(deaths_per_country)/SUM([population])) * 100, 2) percentage_deaths
FROM t1
GROUP BY continent
ORDER BY deaths_by_continent DESC;

-- DEATHS BY INCOME STATUS
SELECT
	[location] AS income_status,
	[population],
	MAX(total_cases) AS total_cases,
	ROUND((MAX(total_cases)/[population]) * 100, 2) as percentage_cases,
	MAX(total_deaths) AS total_deaths,
	ROUND((MAX(total_deaths)/population) * 100, 2) as percentage_deaths
FROM Deaths
WHERE [location] LIKE '%income%'
GROUP BY [location], [population]
ORDER BY percentage_deaths DESC;

