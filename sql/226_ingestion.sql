USE USER_DB_COBRA;
USE SCHEMA RAW;

-- Clear final table before reloading to prevent duplicating
TRUNCATE TABLE USER_DB_COBRA.RAW.NETFLIX_TITLES;

-- Load raw data directly, no transformations (dbt handles cleaning)
COPY INTO USER_DB_COBRA.RAW.NETFLIX_TITLES
FROM (
  SELECT
    $1,   -- show_id
    $2,   -- type
    $3,   -- title
    $7,   -- date_added
    $8,   -- release_year
    $9,   -- rating
    $10,  -- duration
    $11,  -- listed_in
    $6    -- country
  FROM @USER_DB_COBRA.RAW.NETFLIX_STAGE/netflix_titles.csv
)
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
)
ON_ERROR = 'CONTINUE';

-- Verify
SELECT COUNT(*) FROM USER_DB_COBRA.RAW.NETFLIX_TITLES;
SELECT * FROM USER_DB_COBRA.RAW.NETFLIX_TITLES LIMIT 5;
SELECT COUNT(*) FROM USER_DB_COBRA.RAW.NETFLIX_TITLES 
WHERE show_id IS NULL OR title IS NULL;

LIST @USER_DB_COBRA.RAW.NETFLIX_STAGE;

select * from netflix_titles;
