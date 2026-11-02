-- =============================================================================
-- Table: dim_date
-- Description: Financial Trading Calendar Dimension
-- Grain: One row per calendar date
-- Clustering: year, quarter
-- =============================================================================

CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.dim_date`
(
    date_key INT64 NOT NULL OPTIONS(description="Surrogate primary key formatted as YYYYMMDD integer"),
    calendar_date DATE NOT NULL OPTIONS(description="Calendar date (YYYY-MM-DD)"),
    year INT64 NOT NULL OPTIONS(description="Calendar year"),
    quarter INT64 NOT NULL OPTIONS(description="Calendar quarter (1 to 4)"),
    month INT64 NOT NULL OPTIONS(description="Calendar month number (1 to 12)"),
    month_name STRING NOT NULL OPTIONS(description="Full name of the calendar month"),
    day_of_month INT64 NOT NULL OPTIONS(description="Day of month (1 to 31)"),
    day_of_week INT64 NOT NULL OPTIONS(description="Day of week (1=Monday .. 7=Sunday)"),
    day_name STRING NOT NULL OPTIONS(description="Day name"),
    week_of_year INT64 NOT NULL OPTIONS(description="ISO week number (1 to 53)"),
    is_weekend BOOL NOT NULL OPTIONS(description="Flag indicating Saturday or Sunday"),
    is_trading_day BOOL NOT NULL OPTIONS(description="Flag indicating active market trading session"),
    is_us_holiday BOOL NOT NULL OPTIONS(description="Flag indicating US Federal or exchange holiday"),
    fiscal_year INT64 NOT NULL OPTIONS(description="Corporate fiscal accounting year"),
    fiscal_quarter INT64 NOT NULL OPTIONS(description="Corporate fiscal accounting quarter")
)
CLUSTER BY year, quarter
OPTIONS (
    description = "Dimension table maintaining financial calendar attributes, trading holidays, and fiscal periods."
);
