#FOR Q5:
WITH quarterly_revenue AS (
    SELECT
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        service_type,
        SUM(total_amount) AS total_revenue
    FROM
        {{ ref('fct_taxi_trips') }}
    WHERE
        pickup_datetime >= '2019-01-01'
    GROUP BY
        year, quarter, service_type
),
revenue_growth AS (
    SELECT
        this_year.year,
        this_year.quarter,
        this_year.service_type,
        this_year.total_revenue AS current_year_revenue,
        COALESCE(last_year.total_revenue, 0) AS last_year_revenue,
        (this_year.total_revenue - COALESCE(last_year.total_revenue, 0)) / COALESCE(last_year.total_revenue, 1) * 100 AS yoy_growth
    FROM
        quarterly_revenue AS this_year
    LEFT JOIN
        quarterly_revenue AS last_year
    ON
        this_year.service_type = last_year.service_type
        AND this_year.quarter = last_year.quarter
        AND this_year.year = last_year.year + 1
)
SELECT
    year,
    quarter,
    service_type,
    yoy_growth
FROM
    revenue_growth
ORDER BY
    year, quarter, service_type;


#for Q6:
WITH filtered_data AS (
    SELECT
        fare_amount,
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month
    FROM
        {{ ref('fct_taxi_trips') }}
    WHERE
        fare_amount > 0
        AND trip_distance > 0
        AND payment_type_description IN ('Cash', 'Credit Card')
),
percentiles AS (
    SELECT
        service_type,
        year,
        month,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(97)] AS p97,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(95)] AS p95,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(90)] AS p90
    FROM
        filtered_data
    GROUP BY
        service_type, year, month
)
SELECT
    service_type,
    year,
    month,
    p97,
    p95,
    p90
FROM
    percentiles
WHERE
    year = 2020
    AND month = 4
ORDER BY
    service_type;


#For Q7:
WITH trip_duration_data AS (
    SELECT
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        pickup_location,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM
        {{ ref('dim_fhv_trips') }}
    WHERE
        pickup_location IN ('Newark Airport', 'SoHo', 'Yorkville East')
        AND EXTRACT(YEAR FROM pickup_datetime) = 2019
        AND EXTRACT(MONTH FROM pickup_datetime) = 11
),
percentiles AS (
    SELECT
        year,
        month,
        pickup_location,
        APPROX_QUANTILES(trip_duration, 100)[OFFSET(90)] AS p90
    FROM
        trip_duration_data
    GROUP BY
        year, month, pickup_location
)
SELECT
    year,
    month,
    pickup_location,
    p90
FROM
    percentiles
ORDER BY
    pickup_location;
