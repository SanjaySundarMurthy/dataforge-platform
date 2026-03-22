{% macro generate_date_spine(start_date, end_date) %}
/*
  Macro: generate_date_spine
  ──────────────────────────
  Generates a continuous series of dates for the dim_date table.
  This is a common pattern for populating date dimension tables.
*/
WITH date_series AS (
    SELECT generate_series(
        '{{ start_date }}'::date,
        '{{ end_date }}'::date,
        '1 day'::interval
    )::date AS full_date
)

SELECT
    TO_CHAR(full_date, 'YYYYMMDD')::INT AS date_key,
    full_date,
    EXTRACT(YEAR FROM full_date)::INT AS year,
    EXTRACT(QUARTER FROM full_date)::INT AS quarter,
    EXTRACT(MONTH FROM full_date)::INT AS month,
    TO_CHAR(full_date, 'Month') AS month_name,
    EXTRACT(WEEK FROM full_date)::INT AS week,
    EXTRACT(DAY FROM full_date)::INT AS day_of_month,
    EXTRACT(DOW FROM full_date)::INT AS day_of_week,
    TO_CHAR(full_date, 'Day') AS day_name,
    EXTRACT(DOW FROM full_date) IN (0, 6) AS is_weekend

FROM date_series
{% endmacro %}
