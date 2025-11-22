USE DATABASE SAKILA_MIGRATION;
USE SCHEMA PUBLIC;

-- Create GCS integration 

CREATE OR REPLACE STORAGE INTEGRATION GCS_SAKILA_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://sakila-data-bucket/')
  COMMENT = 'Integration for Sakila migration GCS bucket';

-- Create CSV file format

CREATE OR REPLACE FILE FORMAT PUBLIC.sakila_csv_flex
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  SKIP_HEADER = 1
  EMPTY_FIELD_AS_NULL = TRUE
  NULL_IF = ('', 'NULL', 'null', 'N', '"N"')
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  ESCAPE_UNENCLOSED_FIELD = NONE;

-- Create stage (connects to bucket)

CREATE OR REPLACE STAGE PUBLIC.sakila_stage
  STORAGE_INTEGRATION = GCS_SAKILA_INTEGRATION
  URL = 'gcs://sakila-data-bucket/'
  FILE_FORMAT = PUBLIC.sakila_csv_flex;

-- dim_customer
CREATE OR REPLACE TABLE dim_customer (
  customer_key INT,
  location_address_key INT,
  customer_last_update TIMESTAMP_NTZ,
  customer_id INT,
  customer_first_name STRING,
  customer_last_name STRING,
  customer_email STRING,
  customer_active BOOLEAN,
  customer_created TIMESTAMP_NTZ,
  customer_version_number INT,
  customer_valid_from TIMESTAMP_NTZ,
  customer_valid_through TIMESTAMP_NTZ
);
COPY INTO dim_customer
FROM @sakila_stage/dim_customer_fixed.csv
FILE_FORMAT = (FORMAT_NAME = PUBLIC.sakila_csv_flex)
ON_ERROR = 'CONTINUE';


-- dim_film
CREATE OR REPLACE TABLE dim_film (
  film_key INT,
  film_id INT,
  title STRING,
  description STRING,
  release_year INT,
  language STRING,
  rental_duration INT,
  rental_rate FLOAT,
  length INT,
  replacement_cost FLOAT,
  rating STRING,
  special_features STRING,
  last_update TIMESTAMP_NTZ
);
COPY INTO dim_film
FROM @sakila_stage/dim_film.csv
FILE_FORMAT = (FORMAT_NAME = PUBLIC.sakila_csv_flex)
ON_ERROR = 'CONTINUE';

-- dim_location_city
CREATE OR REPLACE TABLE dim_location_city (
  city_key INT,
  city_id INT,
  city STRING,
  country_key INT,
  last_update TIMESTAMP_NTZ
);
COPY INTO dim_location_city
FROM @sakila_stage/dim_location_city.csv
FILE_FORMAT = (FORMAT_NAME = PUBLIC.sakila_csv_flex)
ON_ERROR = 'CONTINUE';

-- numbers
CREATE OR REPLACE TABLE numbers (n INT);
COPY INTO numbers
FROM @sakila_stage/numbers.csv
FILE_FORMAT = (FORMAT_NAME = PUBLIC.sakila_csv_flex)
ON_ERROR = 'CONTINUE';

-- numbers_small
CREATE OR REPLACE TABLE numbers_small (n INT);
COPY INTO numbers_small
FROM @sakila_stage/numbers_small.csv
FILE_FORMAT = (FORMAT_NAME = PUBLIC.sakila_csv_flex)
ON_ERROR = 'CONTINUE';


--  Verify row counts

SELECT TABLE_NAME, ROW_COUNT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'PUBLIC'
ORDER BY TABLE_NAME;

CREATE OR REPLACE FILE FORMAT PUBLIC.sakila_csv_utf8
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL', 'null', 'N', '"N"')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE OR REPLACE TABLE DIM_FILM (
  film_key INT,
  film_last_update TIMESTAMP_NTZ,
  film_id INT,
  film_title STRING,
  film_description STRING,
  film_release_year INT,
  film_language STRING,
  film_rental_duration INT,
  film_rental_rate FLOAT,
  film_duration INT,
  film_replacement_cost FLOAT,
  film_rating_code STRING,
  film_rating_text STRING,
  film_has_trailers BOOLEAN,
  film_has_commentaries BOOLEAN,
  film_has_deleted_scenes BOOLEAN,
  film_has_behind_the_scenes BOOLEAN,
  film_in_category_action BOOLEAN,
  film_in_category_animation BOOLEAN,
  film_in_category_children BOOLEAN,
  film_in_category_classics BOOLEAN,
  film_in_category_comedy BOOLEAN,
  film_in_category_documentary BOOLEAN,
  film_in_category_drama BOOLEAN,
  film_in_category_family BOOLEAN,
  film_in_category_foreign BOOLEAN,
  film_in_category_games BOOLEAN,
  film_in_category_horror BOOLEAN,
  film_in_category_music BOOLEAN,
  film_in_category_new BOOLEAN,
  film_in_category_scifi BOOLEAN,
  film_in_category_sports BOOLEAN,
  film_in_category_travel BOOLEAN
);

COPY INTO DIM_FILM
FROM @sakila_stage/dim_film.csv
FILE_FORMAT = (FORMAT_NAME = PUBLIC.sakila_csv_utf8)
ON_ERROR = 'CONTINUE';


CREATE OR REPLACE TABLE dim_actor (
  actor_key INT,
  actor_last_update TIMESTAMP_NTZ,
  actor_id INT,
  actor_last_name STRING,
  actor_first_name STRING
);

COPY INTO dim_actor
FROM @sakila_stage/dim_actor.csv
FILE_FORMAT = (FORMAT_NAME = PUBLIC.sakila_csv_utf8)
ON_ERROR = 'CONTINUE';


-- dim_date

CREATE OR REPLACE TABLE dim_date (
  date_key INT,
  "date" DATE,
  "timestamp" TIMESTAMP_NTZ,
  weekend STRING,                  -- e.g., 'Weekend' or 'Weekday'
  day_of_week STRING,
  month STRING,                    -- changed from INT → STRING
  month_day INT,
  year INT,
  week_starting_monday DATE
);

COPY INTO dim_date
FROM @sakila_stage/dim_date.csv
FILE_FORMAT = (FORMAT_NAME = PUBLIC.sakila_csv_utf8)
ON_ERROR = 'CONTINUE';


-- dim_film_actor_bridge

CREATE OR REPLACE FILE FORMAT PUBLIC.sakila_csv_repaired
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ESCAPE = '\\'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL', 'null', 'N', '"N"', '"N', 'N,')
  EMPTY_FIELD_AS_NULL = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  REPLACE_INVALID_CHARACTERS = TRUE
  ENCODING = 'UTF8'
  VALIDATE_UTF8 = TRUE;

CREATE OR REPLACE TABLE dim_film_actor_bridge (
  film_key INT,
  actor_key INT,
  actor_weighing_factor FLOAT
);

COPY INTO dim_film_actor_bridge
FROM @sakila_stage/dim_film_actor_bridge_cleaned.csv
FILE_FORMAT = (FORMAT_NAME = PUBLIC.sakila_csv_repaired)
ON_ERROR = 'CONTINUE';


-- dim_location_country

CREATE OR REPLACE TABLE dim_location_country (
  location_country_key INT,
  location_country_id INT,
  location_country_last_update TIMESTAMP_NTZ,
  location_country_name STRING
);

COPY INTO dim_location_country
FROM @sakila_stage/dim_location_country.csv
FILE_FORMAT = (FORMAT_NAME = PUBLIC.sakila_csv_utf8)
ON_ERROR = 'CONTINUE';



-- dim_staff

CREATE OR REPLACE FILE FORMAT PUBLIC.sakila_csv_final
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ESCAPE_UNENCLOSED_FIELD = NONE
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL', 'null', 'N', '"N"', 'N,')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  REPLACE_INVALID_CHARACTERS = TRUE
  ENCODING = 'UTF8';


CREATE OR REPLACE TABLE dim_staff (
  staff_key INT,
  staff_last_update TIMESTAMP_NTZ,
  staff_id INT,
  staff_first_name STRING,
  staff_last_name STRING,
  staff_store_id INT,
  staff_version_number STRING,
  staff_valid_from STRING,
  staff_valid_through STRING,
  staff_active BOOLEAN
);

COPY INTO dim_staff
FROM @sakila_stage/dim_staff_cleaned.csv
FILE_FORMAT = (FORMAT_NAME = PUBLIC.sakila_csv_final)
ON_ERROR = 'CONTINUE';

-- dim_store
CREATE OR REPLACE TABLE dim_store (
  store_key INT,
  location_address_key INT,
  store_last_update STRING,   -- change from TIMESTAMP_NTZ → STRING
  store_id INT,
  store_manager_staff_id INT,
  store_manager_first_name STRING,
  store_manager_last_name STRING,
  store_version_number STRING,
  store_valid_from STRING,
  store_valid_through STRING
);

COPY INTO dim_store
FROM @sakila_stage/dim_store_cleaned.csv
FILE_FORMAT = (FORMAT_NAME = PUBLIC.sakila_csv_final)
ON_ERROR = 'CONTINUE';

-- fact_rental
CREATE OR REPLACE TABLE fact_rental (
  rental_id INT,
  rental_last_update TIMESTAMP_NTZ,
  customer_key INT,
  staff_key INT,
  film_key INT,
  store_key INT,
  rental_date_key INT,
  return_date_key INT,
  count_rentals INT,
  count_returns INT,
  rental_duration INT,
  dollar_amount FLOAT
);

COPY INTO fact_rental
FROM @sakila_stage/fact_rental.csv
FILE_FORMAT = (FORMAT_NAME = PUBLIC.sakila_csv_utf8)
ON_ERROR = 'CONTINUE';


-- VALIDATION
SAKILA_MIGRATIONSELECT TABLE_NAME, ROW_COUNT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'PUBLIC'
ORDER BY TABLE_NAME;
SELECT
    METADATA$FILENAME AS file_name,
    METADATA$FILE_ROW_NUMBER AS line_number,
    $1 AS col1,
    $2 AS col2,
    $3 AS col3,
    $4 AS col4,
    $5 AS col5,
    $6 AS col6
FROM @sakila_stage/dim_film.csv
(FILE_FORMAT => PUBLIC.sakila_csv_flex)
LIMIT 10;

SELECT $1
FROM @sakila_stage/dim_film.csv
(FILE_FORMAT => (TYPE='CSV' FIELD_DELIMITER='\t' SKIP_HEADER=1))
LIMIT 3;

ALTER DATABASE SAKILA_MIGRATION RENAME TO SAKILA_DW;

CREATE OR REPLACE STAGE SAKILA_STAGE
  STORAGE_INTEGRATION = GCS_SAKILA_INTEGRATION
  URL = 'gcs://sakila-data-bucket/'
  FILE_FORMAT = SAKILA_CSV_FORMAT;

CREATE OR REPLACE TABLE DIM_LOCATION_ADDRESS (
    LOCATION_ADDRESS_KEY INT,
    LOCATION_CITY_KEY INT,
    LOCATION_ADDRESS_ID INT,
    LOCATION_ADDRESS_LAST_UPDATE TIMESTAMP_NTZ,
    LOCATION_ADDRESS VARCHAR(64),
    LOCATION_ADDRESS_POSTAL_CODE VARCHAR(10)
);

-- Replace the file format
CREATE OR REPLACE FILE FORMAT SAKILA_CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL', 'N', '')
  EMPTY_FIELD_AS_NULL = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  TRIM_SPACE = TRUE
  DATE_FORMAT = 'YYYY-MM-DD'
  TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS';

-- Load location_address data from GCS bucket
COPY INTO DIM_LOCATION_ADDRESS
FROM @SAKILA_STAGE/dim_location_address.csv
FILE_FORMAT = (FORMAT_NAME = SAKILA_CSV_FORMAT)
ON_ERROR = 'CONTINUE';


-- Verify the load
SELECT COUNT(*) AS ROW_COUNT FROM DIM_LOCATION_ADDRESS;
SELECT * FROM DIM_LOCATION_ADDRESS LIMIT 10;