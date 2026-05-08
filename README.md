# Netflix vs TMDb Trends Analysis Pipeline

## Overview

This project builds an end-to-end data pipeline to analyze the relationship between Netflix’s content catalog and real-time trending movies from TMDb.

We use a modern data stack:
- Airflow for data ingestion and orchestration  
- Snowflake for data storage  
- dbt for data transformation and analytics modeling  
- Preset (Apache Superset) for dashboard visualization  

---

## Objective

This project answers key questions:

- What genres are trending vs what Netflix offers?
- How do trending metrics change over time?

---

## Architecture

### Data Sources
- Netflix dataset (`netflix_titles.csv`)
- TMDb API
  - Trending movies
  - Genre list

---

## Airflow Pipeline

The DAG `tmdb_realtime_ingest` performs:

1. Extract TMDb trending data  
2. Extract TMDb genres  
3. Load data into Snowflake RAW layer  
4. Run dbt transformations  
5. Create dashboard views  
6. Populate final dashboard table  

---

## Data Layers

### RAW Layer (Snowflake)
Stores unprocessed data:
- `RAW.NETFLIX_TITLES`
- `RAW.TMDB_TRENDING`
- `RAW.TMDB_GENRES`

### Staging Layer (dbt)
Basic cleaning and standardization:
- `stg_netflix_titles`
- `stg_tmdb_trending`
- `stg_tmdb_genres`

### Intermediate Layer (dbt)
Data reshaping and transformations:
- `int_genre_netflix`
- `int_tmdb_trending_genres`

### Analytics Layer (dbt)

Core business logic and insights:

#### Fact Tables
- `FACT_NETFLIX_CATALOG`
- `FACT_TMDB_TRENDING_SNAPSHOT`

#### Dimension Tables
- `DIM_TITLE`
- `DIM_GENRE`

#### Marts
- `MART_GENRE_GAP_ANALYSIS`
- `MART_KPIS_DAILY`

###.env file is needed to run dbt
---

## Key Metrics

### MART_KPIS_DAILY
- Average popularity  
- Average rating  
- Top genre  
- Number of trending titles  

### MART_GENRE_GAP_ANALYSIS
- Netflix genre share vs trending genre share  
- Genre demand gap  

---

## Dashboard Layer

Snowflake views used for BI:
- `DASHBOARD.KPIS_DAILY`
- `DASHBOARD.GENRE_GAP_ANALYSIS`
- `DASHBOARD.NETFLIX_CATALOG`
- `DASHBOARD.TMDB_TRENDING_SNAPSHOT`

These power dashboards in Preset (Apache Superset):
- Genre gap analysis
- Daily KPI tracking

---

## Time-Series Design

TMDb only provides current trending data.

To simulate historical trends:
- Airflow uses logical execution dates  
- Each run is stored with a timestamp  
- Backfill creates a time-series dataset  

---

## Tech Stack

- Python  
- Airflow  
- Snowflake  
- dbt  
- Preset (Apache Superset)  

---

## Key Takeaways

- Airflow handles data ingestion and orchestration  
- dbt handles data transformation and analytics modeling  
- Snowflake serves as the centralized data warehouse  
- The pipeline demonstrates a modern data engineering workflow  

---

## Team

- Alyssa Gomez
- Sang Ah Lee
- Ananya Yallapragada
- Angela Wei
