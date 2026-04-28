USE USER_DB_COBRA;
USE SCHEMA RAW;

-- Messy genre version for temporary table
COPY INTO USER_DB_COBRA.RAW.NETFLIX_STAGING_ZONE
FROM (
  SELECT $1, $2, $3, $7, $8, $9, $10, $11, $6 
  FROM @USER_DB_COBRA.RAW.NETFLIX_STAGE/netflix_titles.csv
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- Clear final table before reloading to prevent duplicating
TRUNCATE TABLE USER_DB_COBRA.RAW.NETFLIX_TITLES;

COPY INTO USER_DB_COBRA.RAW.NETFLIX_STAGING_ZONE
FROM @USER_DB_COBRA.RAW.NETFLIX_STAGE/netflix_titles.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- Add into the new table
INSERT INTO USER_DB_COBRA.RAW.NETFLIX_TITLES (
    show_id, type, title, date_added, release_year, rating, duration, listed_in, country
)
SELECT 
    show_id, 
    type, 
    title, 
    date_added, 
    release_year, 
    rating, 
    -- 1. Remove ' min'
    -- 2. Cast as INT
    CAST(REPLACE(duration, ' min', '') AS INT), 
    TRIM(f_genre.value::string),   
    TRIM(f_country.value::string) 
FROM USER_DB_COBRA.RAW.NETFLIX_STAGING_ZONE,
LATERAL FLATTEN(input => SPLIT(listed_in, ',')) f_genre,
LATERAL FLATTEN(input => SPLIT(IFNULL(country, 'Unknown'), ',')) f_country
WHERE type = 'Movie';

-- Clean up the temporary table
TRUNCATE TABLE USER_DB_COBRA.RAW.NETFLIX_STAGING_ZONE;

-- Verify
SELECT COUNT(*), title FROM USER_DB_COBRA.RAW.NETFLIX_TITLES GROUP BY title HAVING COUNT(*) > 1 LIMIT 5;
SELECT * From NETFLIX_TITLES LIMIT 5;