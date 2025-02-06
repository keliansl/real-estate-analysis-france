-- -----------------------------------------------------------------------------------
-- Create a table aggregating real estate prices per department (houses only)
-- Computes median and average property values and price per square meter
-- This dataset do not include outliers.
-- -----------------------------------------------------------------------------------
CREATE TABLE `prix_immo.agg2_valeurs_foncieres_clean_maison` AS
SELECT 
  department_number,
  -- Format department names properly
  ANY_VALUE(
    ARRAY_TO_STRING(
      ARRAY(
        SELECT CONCAT(UPPER(SUBSTR(word, 1, 1)), LOWER(SUBSTR(word, 2)))
        FROM UNNEST(SPLIT(department_name, '-')) AS word
      ), '-'
    )
  ) AS department_name,

  -- Compute median and average real estate values and price per square meter
  ROUND(APPROX_QUANTILES(valeur_fonciere, 100)[OFFSET(50)], 2) AS median_valeur_fonciere,
  ROUND(APPROX_QUANTILES(prix_m2, 100)[OFFSET(50)], 2) AS median_prix_m2,
  ROUND(AVG(valeur_fonciere), 2) AS mean_valeur_fonciere,
  ROUND(AVG(prix_m2), 2) AS mean_prix_m2
FROM `prix_immo.valeurs_foncieres_clean6_maison`
GROUP BY department_number
ORDER BY department_number;

-- -----------------------------------------------------------------------------------
-- Create a table aggregating real estate prices per department (houses only)
-- Computes median and average property values and price per square meter
-- This dataset includes outliers.
-- -----------------------------------------------------------------------------------
CREATE TABLE `prix_immo.agg2_valeurs_foncieres_clean4_maison` AS
SELECT 
  department_number,
  -- Format department names properly
  ANY_VALUE(
    ARRAY_TO_STRING(
      ARRAY(
        SELECT CONCAT(UPPER(SUBSTR(word, 1, 1)), LOWER(SUBSTR(word, 2)))
        FROM UNNEST(SPLIT(department_name, '-')) AS word
      ), '-'
    )
  ) AS department_name,

  -- Compute median and average real estate values and price per square meter
  ROUND(APPROX_QUANTILES(valeur_fonciere, 100)[OFFSET(50)], 2) AS median_valeur_fonciere,
  ROUND(APPROX_QUANTILES(prix_m2, 100)[OFFSET(50)], 2) AS median_prix_m2,
  ROUND(AVG(valeur_fonciere), 2) AS mean_valeur_fonciere,
  ROUND(AVG(prix_m2), 2) AS mean_prix_m2
FROM `prix_immo.valeurs_foncieres_clean4_maison`
GROUP BY department_number
ORDER BY department_number;

-- -----------------------------------------------------------------------------------
-- Create a table aggregating rental prices per department (houses only)
-- Computes median and average rent per square meter
-- -----------------------------------------------------------------------------------
CREATE TABLE `prix_immo.agg_loyer_maison_clean_ville` AS
SELECT 
  department_number,
  ANY_VALUE(
    ARRAY_TO_STRING(
      ARRAY(
        SELECT CONCAT(UPPER(SUBSTR(word, 1, 1)), LOWER(SUBSTR(word, 2)))
        FROM UNNEST(SPLIT(department_name, '-')) AS word
      ), '-'
    )
  ) AS department_name,

  -- Compute median and average rent per square meter
  ROUND(AVG(loypredm2), 2) AS mean_loyer_maison_m2,
  ROUND(APPROX_QUANTILES(loypredm2, 100)[OFFSET(50)], 2) AS median_loyer_maison_m2
FROM `prix_immo.loyer_maison_clean_ville`
GROUP BY department_number;

-- -----------------------------------------------------------------------------------
-- Join real estate prices with rental prices to compute rental yield per department
-- Rental yield = (Annual rent / Property price) * 100
-- -----------------------------------------------------------------------------------
SELECT 
  vf.department_number,
  CONCAT('FR-', LPAD(vf.department_number, 2, '0')) AS geo_department,
  vf.department_name,
  'France' AS country,
  vf.median_valeur_fonciere,
  vf.median_prix_m2,
  vf.mean_valeur_fonciere,
  vf.mean_prix_m2,
  loy.median_loyer_maison_m2,
  loy.mean_loyer_maison_m2,

  -- Compute rental yield (profitability)
  ROUND((loy.median_loyer_maison_m2 * 12 / vf.median_prix_m2) * 100, 2) AS median_rentabilite,
  ROUND((loy.mean_loyer_maison_m2 * 12 / vf.mean_prix_m2) * 100, 2) AS mean_rentabilite
FROM `prix_immo.agg_valeurs_foncieres_clean_maison` AS vf
LEFT JOIN `prix_immo.agg_loyer_maison_clean_ville` AS loy
ON vf.department_number = loy.department_number
ORDER BY vf.department_number;

-- =========================================================================================
-- Query: Compute Real Estate Profitability by Department in France (Without Outliers)
-- -----------------------------------------------------------------------------------
-- This query calculates the median and mean real estate values, rental prices, and 
-- profitability for each department in France. It joins real estate transaction data 
-- with rental data, this dataset do not include outliers.
-- -----------------------------------------------------------------------------------

SELECT 
  vf.department_number, -- Department number (e.g., 75 for Paris, 69 for Lyon)
  
  -- Create a geographic department code in the format "FR-XX" (e.g., "FR-75" for Paris)
  CONCAT('FR-', LPAD(vf.department_number, 2, '0')) AS geo_department,

  vf.department_name, -- Name of the department
  'France' AS country, -- Static value to specify the country
  
  vf.nombre_ventes, -- Number of real estate transactions in the department
  
  -- Median and mean real estate prices per department
  vf.median_valeur_fonciere, -- Median property value
  vf.median_prix_m2, -- Median price per square meter
  vf.mean_valeur_fonciere, -- Mean property value
  vf.mean_prix_m2, -- Mean price per square meter

  -- Median and mean rental prices per square meter
  loy.median_loyer_maison_m2, -- Median rent per square meter
  loy.mean_loyer_maison_m2, -- Mean rent per square meter

  -- Compute annual rental profitability (%) = (Annual Rent / Property Price) * 100
  ROUND((loy.median_loyer_maison_m2 * 12 / vf.median_prix_m2) * 100, 2) AS median_rentabilite,
  ROUND((loy.mean_loyer_maison_m2 * 12 / vf.mean_prix_m2) * 100, 2) AS mean_rentabilite

FROM `prix_immo.agg2_valeurs_foncieres_clean_maison` AS vf  -- Table containing aggregated property values
LEFT JOIN `prix_immo.agg_loyer_maison_clean_ville` AS loy  -- Table containing aggregated rental prices
ON vf.department_number = loy.department_number  -- Join on department number

ORDER BY vf.department_number;  -- Sort results by department number

-- =========================================================================================
-- Query: Compute Real Estate Profitability by Department in France (With Outliers)
-- -----------------------------------------------------------------------------------
-- This query calculates the median and mean real estate values, rental prices, and 
-- profitability for each department in France. It joins real estate transaction data 
-- with rental data, but this dataset includes outliers.
-- -----------------------------------------------------------------------------------

SELECT  
  vf.department_number, -- Department number (e.g., 75 for Paris, 69 for Lyon)

  -- Create a geographic department code in the format "FR-XX" (e.g., "FR-75" for Paris)
  CONCAT('FR-', LPAD(vf.department_number, 2, '0')) AS geo_department,

  vf.department_name, -- Name of the department
  'France' AS country, -- Static value to specify the country

  vf.nombre_ventes, -- Number of real estate transactions in the department

  -- Median and mean real estate prices per department
  vf.median_valeur_fonciere, -- Median property value
  vf.median_prix_m2, -- Median price per square meter
  vf.mean_valeur_fonciere, -- Mean property value
  vf.mean_prix_m2, -- Mean price per square meter

  -- Median and mean rental prices per square meter
  loy.median_loyer_maison_m2, -- Median rent per square meter
  loy.mean_loyer_maison_m2, -- Mean rent per square meter

  -- Compute annual rental profitability (%) = (Annual Rent / Property Price) * 100
  ROUND((loy.median_loyer_maison_m2 * 12 / vf.median_prix_m2) * 100, 2) AS median_rentabilite,
  ROUND((loy.mean_loyer_maison_m2 * 12 / vf.mean_prix_m2) * 100, 2) AS mean_rentabilite

FROM `prix_immo.agg2_valeurs_foncieres_clean4_maison` AS vf  -- Table containing aggregated property values with outliers
LEFT JOIN `prix_immo.agg_loyer_maison_clean_ville` AS loy  -- Table containing aggregated rental prices
ON vf.department_number = loy.department_number  -- Join on department number

ORDER BY vf.department_number;  -- Sort results by department number
