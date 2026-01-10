CREATE TABLE quantum_business_copy (
    Company_ID INT,
    Year INT,
    Country TEXT,
    Region TEXT,
    Industry TEXT,
    Company_Size_Thousands DOUBLE,
    Annual_Revenue_Billion_USD DOUBLE,
    QC_Investment_Million_USD DOUBLE,
    Quantum_Adoption_Percentage DOUBLE,
    Expected_Profit_Increase_Pct DOUBLE,
    PQC_Migration_Years DOUBLE,
    GSI_Migration_Years DOUBLE,
    User_Sentiment_Mean DOUBLE,
    User_Sentiment_Variance DOUBLE
);
INSERT INTO quantum_business_copy (
    Company_ID, Year, Country, Region, Industry, 
    Company_Size_Thousands, Annual_Revenue_Billion_USD, 
    QC_Investment_Million_USD, Quantum_Adoption_Percentage, 
    Expected_Profit_Increase_Pct, PQC_Migration_Years, 
    GSI_Migration_Years, User_Sentiment_Mean, User_Sentiment_Variance
) VALUES (
    NULL, NULL, NULL, NULL, NULL, 
    NULL, NULL, NULL, NULL, 
    NULL, NULL, NULL, NULL, NULL
);
TRUNCATE TABLE quantum_business_copy;
LOAD DATA LOCAL INFILE 'C:/Users/DELL/Downloads/quantum_business_adoption_dataset.csv'
INTO TABLE quantum_business_copy 
FIELDS TERMINATED BY ',' 
-- Removed ENCLOSED BY because your CSV doesn't use quotes
LINES TERMINATED BY '\n' -- Changed from \r\n to \n based on the file content
IGNORE 1 LINES;
select count(*) from quantum_business_copy;


-- Identify duplicate rows using ROW_NUMBER

select *, row_number() OVER (
               PARTITION BY 
                   Company_ID, Year, Country, Region, Industry, 
                   Company_Size_Thousands, Annual_Revenue_Billion_USD, 
                   QC_Investment_Million_USD, Quantum_Adoption_Percentage, 
                   Expected_Profit_Increase_Pct, PQC_Migration_Years, 
                   GSI_Migration_Years, User_Sentiment_Mean, User_Sentiment_Variance,
                   Company_ID) as row_count from quantum_business_copy; 
                   
with duplicates_cte as(	select *, row_number() OVER (
               PARTITION BY 
                   Company_ID, Year, Country, Region, Industry, 
                   Company_Size_Thousands, Annual_Revenue_Billion_USD, 
                   QC_Investment_Million_USD, Quantum_Adoption_Percentage, 
                   Expected_Profit_Increase_Pct, PQC_Migration_Years, 
                   GSI_Migration_Years, User_Sentiment_Mean, User_Sentiment_Variance,
                   Company_ID) as row_count from quantum_business_copy) select * from duplicates_cte 
                   where row_count>1; 		
-- no exact duplicates in the function 
-- filling the null values 
                   


SELECT
    COUNT(*) AS total_rows,
    COUNT(Country) AS count_country,
    COUNT(NULLIF(Country, '')) AS count_country_nullif
FROM quantum_business_copy;

SELECT
    COUNT(*) - COUNT(NULLIF(Company_ID, NULL)) AS Company_ID_missing,
    COUNT(*) - COUNT(NULLIF(Year, '')) AS Year_missing,
    COUNT(*) - COUNT(NULLIF(Country, '')) AS Country_missing,
    COUNT(*) - COUNT(NULLIF(Region, '')) AS Region_missing,
    COUNT(*) - COUNT(NULLIF(Industry, '')) AS Industry_missing,
    COUNT(*) - COUNT(NULLIF(Company_Size_Thousands, '')) AS Company_Size_missing,
    COUNT(*) - COUNT(NULLIF(Annual_Revenue_Billion_USD, '')) AS Revenue_missing,
    COUNT(*) - COUNT(NULLIF(QC_Investment_Million_USD, '')) AS QC_Investment_missing,
    COUNT(*) - COUNT(NULLIF(Quantum_Adoption_Percentage, '')) AS Adoption_missing,
    COUNT(*) - COUNT(NULLIF(Expected_Profit_Increase_Pct, '')) AS Profit_Increase_missing,
    COUNT(*) - COUNT(NULLIF(PQC_Migration_Years, '')) AS PQC_Migration_missing,
    COUNT(*) - COUNT(NULLIF(GSI_Migration_Years, '')) AS GSI_Migration_missing,
    COUNT(*) - COUNT(NULLIF(User_Sentiment_Mean, '')) AS Sentiment_Mean_missing,
    COUNT(*) - COUNT(NULLIF(User_Sentiment_Variance, '')) AS Sentiment_Variance_missing
FROM quantum_business_copy;
SELECT
    Company_ID,
    Year,
    Country
FROM quantum_business_copy
WHERE Company_ID = 0;
WITH OrderedQC AS (
    SELECT
        QC_Investment_Million_USD,
        ROW_NUMBER() OVER (ORDER BY QC_Investment_Million_USD) AS rn,
        COUNT(*) OVER () AS total_rows
    FROM quantum_business_copy
    WHERE QC_Investment_Million_USD IS NOT NULL
)
SELECT AVG(QC_Investment_Million_USD) AS median_qc_investment
FROM OrderedQC
WHERE rn IN (
    FLOOR((total_rows + 1) / 2),
    CEIL((total_rows + 1) / 2)
);


WITH MedianValue AS (
    SELECT AVG(QC_Investment_Million_USD) AS median_qc
    FROM (
        SELECT
            QC_Investment_Million_USD,
            ROW_NUMBER() OVER (ORDER BY QC_Investment_Million_USD) AS rn,
            COUNT(*) OVER () AS total_rows
        FROM quantum_business_copy
        WHERE QC_Investment_Million_USD IS NOT NULL
    ) t
    WHERE rn IN (
        FLOOR((total_rows + 1) / 2),
        CEIL((total_rows + 1) / 2)
    )
)

UPDATE quantum_business_copy
SET QC_Investment_Million_USD = (SELECT median_qc FROM MedianValue)
WHERE QC_Investment_Million_USD=0;
SET SQL_SAFE_UPDATES = 0;
-- VERFIY 
SELECT 
 COUNT(*) - COUNT(NULLIF(QC_Investment_Million_USD, '')) AS QC_Investment_missing FROM quantum_business_copy;
 WITH MedianValue2 AS (
    SELECT AVG(Quantum_Adoption_Percentage) AS median_qc2
    FROM (
        SELECT
            Quantum_Adoption_Percentage,
            ROW_NUMBER() OVER (ORDER BY Quantum_Adoption_Percentage) AS rn,
            COUNT(*) OVER () AS total_rows
        FROM quantum_business_copy
        WHERE Quantum_Adoption_Percentage  IS NOT NULL
    ) t
    WHERE rn IN (
        FLOOR((total_rows + 1) / 2),
        CEIL((total_rows + 1) / 2)
    )
)

UPDATE quantum_business_copy
SET Quantum_Adoption_Percentage = (SELECT median_qc2 FROM MedianValue2)
WHERE Quantum_Adoption_Percentage=0;
WITH MedianValue3 AS (
    SELECT AVG(Expected_Profit_Increase_Pct) AS median_qc3
    FROM (
        SELECT
		   Expected_Profit_Increase_Pct,
            ROW_NUMBER() OVER (ORDER BY Expected_Profit_Increase_Pct) AS rn,
            COUNT(*) OVER () AS total_rows
        FROM quantum_business_copy
        WHERE Expected_Profit_Increase_Pct  IS NOT NULL
    ) t
    WHERE rn IN (
        FLOOR((total_rows + 1) / 2),
        CEIL((total_rows + 1) / 2)
    )
)

UPDATE quantum_business_copy
SET Expected_Profit_Increase_Pct = (SELECT median_qc3 FROM MedianValue3)
WHERE Expected_Profit_Increase_Pct=0;
WITH MedianValue4 AS (
    SELECT AVG(User_Sentiment_Variance) AS median_qc4
    FROM (
        SELECT
		    User_Sentiment_Variance,
            ROW_NUMBER() OVER (ORDER BY User_Sentiment_Variance) AS rn,
            COUNT(*) OVER () AS total_rows
        FROM quantum_business_copy
        WHERE User_Sentiment_Variance IS NOT NULL
    ) t
    WHERE rn IN (
        FLOOR((total_rows + 1) / 2),
        CEIL((total_rows + 1) / 2)
    )
)

UPDATE quantum_business_copy
SET User_Sentiment_Variance = (SELECT median_qc4 FROM MedianValue4)
WHERE User_Sentiment_Variance=0;

ALTER TABLE quantum_business_copy
DROP COLUMN User_Sentiment_Mean;
-- dropping the column cause it is 34% empty and does not hold a strong significance

SELECT
    COUNT(*) - COUNT(NULLIF(Company_ID, NULL)) AS Company_ID_missing,
    COUNT(*) - COUNT(NULLIF(Year, '')) AS Year_missing,
    COUNT(*) - COUNT(NULLIF(Country, '')) AS Country_missing,
    COUNT(*) - COUNT(NULLIF(Region, '')) AS Region_missing,
    COUNT(*) - COUNT(NULLIF(Industry, '')) AS Industry_missing,
    COUNT(*) - COUNT(NULLIF(Company_Size_Thousands, '')) AS Company_Size_missing,
    COUNT(*) - COUNT(NULLIF(Annual_Revenue_Billion_USD, '')) AS Revenue_missing,
    COUNT(*) - COUNT(NULLIF(QC_Investment_Million_USD, '')) AS QC_Investment_missing,
    COUNT(*) - COUNT(NULLIF(Quantum_Adoption_Percentage, '')) AS Adoption_missing,
    COUNT(*) - COUNT(NULLIF(Expected_Profit_Increase_Pct, '')) AS Profit_Increase_missing,
    COUNT(*) - COUNT(NULLIF(PQC_Migration_Years, '')) AS PQC_Migration_missing,
    COUNT(*) - COUNT(NULLIF(GSI_Migration_Years, '')) AS GSI_Migration_missing,
    COUNT(*) - COUNT(NULLIF(User_Sentiment_Variance, '')) AS Sentiment_Variance_missing
FROM quantum_business_copy;
ALTER TABLE quantum_business_copy
ADD COLUMN Company_Size_std DOUBLE,
ADD COLUMN Annual_Revenue_std DOUBLE,
ADD COLUMN QC_Investment_std DOUBLE,
ADD COLUMN Quantum_Adoption_std DOUBLE,
ADD COLUMN Expected_Profit_std DOUBLE,
ADD COLUMN PQC_Migration_std DOUBLE,
ADD COLUMN GSI_Migration_std DOUBLE,
ADD COLUMN User_Sentiment_Variance_std DOUBLE;

-- Updating all z-score columns using a derived table
UPDATE quantum_business_copy q
JOIN (
    SELECT 
        AVG(Company_Size_Thousands) AS mean_size,
        STDDEV_SAMP(Company_Size_Thousands) AS std_size,
        
        AVG(Annual_Revenue_Billion_USD) AS mean_rev,
        STDDEV_SAMP(Annual_Revenue_Billion_USD) AS std_rev,
        
        AVG(QC_Investment_Million_USD) AS mean_qc,
        STDDEV_SAMP(QC_Investment_Million_USD) AS std_qc,
        
        AVG(Quantum_Adoption_Percentage) AS mean_adopt,
        STDDEV_SAMP(Quantum_Adoption_Percentage) AS std_adopt,
        
        AVG(Expected_Profit_Increase_Pct) AS mean_profit,
        STDDEV_SAMP(Expected_Profit_Increase_Pct) AS std_profit,
        
        AVG(PQC_Migration_Years) AS mean_pqc,
        STDDEV_SAMP(PQC_Migration_Years) AS std_pqc,
        
        AVG(GSI_Migration_Years) AS mean_gsi,
        STDDEV_SAMP(GSI_Migration_Years) AS std_gsi,
        
        AVG(User_Sentiment_Variance) AS mean_sent,
        STDDEV_SAMP(User_Sentiment_Variance) AS std_sent
    FROM quantum_business_copy
) stats
SET 
    q.Company_Size_std = (q.Company_Size_Thousands - stats.mean_size) / stats.std_size,
    q.Annual_Revenue_std = (q.Annual_Revenue_Billion_USD - stats.mean_rev) / stats.std_rev,
    q.QC_Investment_std = (q.QC_Investment_Million_USD - stats.mean_qc) / stats.std_qc,
    q.Quantum_Adoption_std = (q.Quantum_Adoption_Percentage - stats.mean_adopt) / stats.std_adopt,
    q.Expected_Profit_std = (q.Expected_Profit_Increase_Pct - stats.mean_profit) / stats.std_profit,
    q.PQC_Migration_std = (q.PQC_Migration_Years - stats.mean_pqc) / stats.std_pqc,
    q.GSI_Migration_std = (q.GSI_Migration_Years - stats.mean_gsi) / stats.std_gsi,
    q.User_Sentiment_Variance_std = (q.User_Sentiment_Variance - stats.mean_sent) / stats.std_sent;


select * from quantum_business_copy;
