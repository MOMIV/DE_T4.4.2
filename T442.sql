CREATE TEMPORARY TABLE IF NOT EXISTS people_tmp ( 
IndexP int, 
UserId String,
FirstName String,
LastName String, 
Sex String, 
Email String, 
Phone String, 
Dateofbirth String, 
JobTitle String, 
GroupNum  String)
COMMENT 'People details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE TEMPORARY TABLE IF NOT EXISTS organizations_tmp ( 
IndexO int, 
OrganizationId String,
Name String,
Website String, 
Country String, 
Description String, 
Founded String, 
Industry String, 
Number_of_employees String, 
GroupNum  String)
COMMENT 'Organizations details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE TEMPORARY TABLE IF NOT EXISTS customers_tmp ( 
IndexC int, 
CustomerId String,
FirstName String,
LastName String,
Company String,
City String, 
Country String, 
Phone1 String, 
Phone2 String,
Email String, 
SubscriptionDate String, 
Website String, 
GroupNum  String,
YearC  String)
COMMENT 'Customers details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count' = '1');

LOAD DATA INPATH '/user/momiv/people_n.csv' OVERWRITE INTO TABLE people_tmp;
LOAD DATA INPATH '/user/momiv/organizations_n.csv' OVERWRITE INTO TABLE organizations_tmp;
LOAD DATA INPATH '/user/momiv/customers_n.csv' OVERWRITE INTO TABLE customers_tmp;

set hive.enforce.bucketing=true;

CREATE TABLE people (
IndexP int, 
UserId String,
FirstName String,
LastName String, 
Sex String, 
Email String, 
Phone String, 
Dateofbirth String, 
AgeGroup String,
JobTitle String, 
GroupNum  String)
CLUSTERED BY(GroupNum) INTO 10 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET;

INSERT INTO people 
SELECT 
IndexP,
UserId,
FirstName,
LastName, 
Sex, 
Email,
Phone, 
Dateofbirth,
CASE 
    WHEN (YEAR(current_date) - YEAR(Dateofbirth)) < 18  THEN '0-18'
    WHEN (YEAR(current_date) - YEAR(Dateofbirth)) > 18 AND (YEAR(current_date) - YEAR(Dateofbirth)) <= 28 THEN '18-28'
    WHEN (YEAR(current_date) - YEAR(Dateofbirth)) > 28 AND (YEAR(current_date) - YEAR(Dateofbirth)) <= 40 THEN '28-40'
    WHEN (YEAR(current_date) - YEAR(Dateofbirth)) > 40 AND (YEAR(current_date) - YEAR(Dateofbirth)) <= 60 THEN '40-60'
    ELSE '60+'
    END AS AgeGroup, 
JobTitle, 
GroupNum 
FROM people_tmp;

DROP table people_tmp;

CREATE TABLE IF NOT EXISTS organizations ( 
IndexO int, 
OrganizationId String,
Name String,
Website String, 
Country String, 
Description String, 
Founded String, 
Industry String, 
Number_of_employees String, 
GroupNum  String)
CLUSTERED BY(GroupNum) INTO 10 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET;

INSERT INTO organizations 
SELECT *
FROM organizations_tmp;

DROP table organizations_tmp;

CREATE TABLE IF NOT EXISTS customers ( 
IndexC int, 
CustomerId String,
FirstName String,
LastName String,
Company String,
City String, 
Country String, 
Phone1 String, 
Phone2 String,
Email String, 
SubscriptionDate String, 
Website String, 
GroupNum  String)
PARTITIONED BY (YearC String)
CLUSTERED BY(GroupNum) INTO 10 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET;

INSERT INTO customers PARTITION (YearC=2020)
SELECT 
IndexC, 
CustomerId,
FirstName,
LastName,
Company,
City, 
Country, 
Phone1, 
Phone2,
Email, 
SubscriptionDate, 
Website, 
GroupNum
FROM customers_tmp WHERE YearC=2020;

INSERT INTO customers PARTITION (YearC=2021)
SELECT 
IndexC, 
CustomerId,
FirstName,
LastName,
Company,
City, 
Country, 
Phone1, 
Phone2,
Email, 
SubscriptionDate, 
Website, 
GroupNum
FROM customers_tmp WHERE YearC=2021;

INSERT INTO customers PARTITION (YearC=2022)
SELECT 
IndexC, 
CustomerId,
FirstName,
LastName,
Company,
City, 
Country, 
Phone1, 
Phone2,
Email, 
SubscriptionDate, 
Website, 
GroupNum
FROM customers_tmp WHERE YearC=2022;

DROP table customers_tmp;

WITH customers_union as (
SELECT CustomerId, FirstName, LastName, Email, Website,YearC
FROM customers
WHERE YearC=2020
UNION ALL
SELECT CustomerId, FirstName, LastName, Email, Website, YearC
FROM customers
WHERE YearC=2021
UNION ALL
SELECT CustomerId, FirstName, LastName, Email, Website, YearC
FROM customers
WHERE YearC=2022
),
customers_count as (
SELECT o.Name , cu.YearC, p.AgeGroup, count(*) as AgeGroupCount 
From organizations o 
join customers_union cu on o.Website=cu.Website
join people p on cu.FirstName=p.FirstName AND cu.LastName=p.LastName AND cu.Email=p.Email
GROUP BY Name, YearC, AgeGroup
),
customers_max as (
SELECT cc.Name, cc.YearC , max(cc.AgeGroupCount) as MC
from customers_count cc
GROUP BY Name, YearC
)
SELECT cc.Name as Company, cc.YearC as Year, cc.AgeGroup as Age_Group
from customers_max cm
join customers_count cc on cm.Name=cc.Name AND cm.YearC=cc.YearC AND cc.AgeGroupCount=cm.MC
ORDER BY Company, Year;

EXPLAIN
WITH customers_union as (
SELECT CustomerId, FirstName, LastName, Email, Website,YearC
FROM customers
WHERE YearC=2020
UNION ALL
SELECT CustomerId, FirstName, LastName, Email, Website, YearC
FROM customers
WHERE YearC=2021
UNION ALL
SELECT CustomerId, FirstName, LastName, Email, Website, YearC
FROM customers
WHERE YearC=2022
),
customers_count as (
SELECT o.Name , cu.YearC, p.AgeGroup, count(*) as AgeGroupCount 
From organizations o 
join customers_union cu on o.Website=cu.Website
join people p on cu.FirstName=p.FirstName AND cu.LastName=p.LastName AND cu.Email=p.Email
GROUP BY Name, YearC, AgeGroup
),
customers_max as (
SELECT cc.Name, cc.YearC , max(cc.AgeGroupCount) as MC
from customers_count cc
GROUP BY Name, YearC
)
SELECT cc.Name as Company, cc.YearC as Year, cc.AgeGroup
from customers_max cm
join customers_count cc on cm.Name=cc.Name AND cm.YearC=cc.YearC AND cc.AgeGroupCount=cm.MC
ORDER BY Company, Year;