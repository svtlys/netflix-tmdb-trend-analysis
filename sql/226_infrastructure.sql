DROP TABLE IF EXISTS USER_DB_COBRA.RAW.NETFLIX_TITLES;
DROP TABLE IF EXISTS USER_DB_COBRA.RAW.TMDB_GENRES;
DROP TABLE IF EXISTS USER_DB_COBRA.RAW.TMDB_TRENDING;

-- Standard Schema for Team
CREATE SCHEMA IF NOT EXISTS USER_DB_COBRA.RAW;
CREATE SCHEMA IF NOT EXISTS USER_DB_COBRA.ANALYTICS;
CREATE SCHEMA IF NOT EXISTS USER_DB_COBRA.DASHBOARD;

-- Create Stage
CREATE STAGE IF NOT EXISTS USER_DB_COBRA.RAW.NETFLIX_STAGE
    FILE_FORMAT = (
        TYPE = 'CSV'
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    );

-- Create RAW table for Netflix titles
CREATE OR REPLACE TABLE USER_DB_COBRA.RAW.NETFLIX_TITLES (
    show_id STRING,
    type STRING,
    title STRING,
    date_added STRING,
    release_year INT,
    rating STRING,
    duration STRING,    -- dbt will cast this to INT
    listed_in STRING,   -- raw comma-separated, dbt will flatten
    country STRING
);

-- Create RAW table for TMDb Trending
CREATE OR REPLACE TABLE USER_DB_COBRA.RAW.TMDB_TRENDING (
    tmdb_id INT,
    title STRING,
    release_date DATE,
    popularity FLOAT,
    vote_average FLOAT,
    vote_count INT,
    genre_ids ARRAY, 
    original_language STRING,
    snapshot_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create RAW table for TMDb Genres
CREATE OR REPLACE TABLE USER_DB_COBRA.RAW.TMDB_GENRES (
    genre_id INT,
    genre_name STRING
);

-- Create Dashboard
CREATE OR REPLACE TABLE USER_DB_COBRA.DASHBOARD.FINAL_NETFLIX_MOVIES (
    show_id STRING,
    type STRING,
    title STRING,
    date_added STRING,
    release_year INT,
    rating STRING,
    duration INT,      
    listed_in STRING, 
    country STRING
);

-- This allows anyone in the account to see and use your database 
GRANT USAGE ON DATABASE USER_DB_COBRA TO ROLE PUBLIC;
GRANT USAGE ON ALL SCHEMAS IN DATABASE USER_DB_COBRA TO ROLE PUBLIC;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA USER_DB_COBRA.RAW TO ROLE PUBLIC;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA USER_DB_COBRA.DASHBOARD TO ROLE PUBLIC;
