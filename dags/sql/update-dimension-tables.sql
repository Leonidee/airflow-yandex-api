START TRANSACTION;
/*
 d_calendar
 */
WITH
    all_dates AS (
        SELECT DISTINCT
            TO_DATE(date_time::TEXT, 'YYYY-MM-DD') AS date_time
        FROM {stage_schema}.user_activity_log
        WHERE date_time::timestamp WITHOUT TIME ZONE NOT IN (
            SELECT
                fact_date
            FROM {mart_schema}.d_calendar
            )
        UNION
        SELECT DISTINCT
            TO_DATE(date_time::TEXT, 'YYYY-MM-DD') AS date_time
        FROM {stage_schema}.user_order_log
        WHERE date_time::timestamp WITHOUT TIME ZONE NOT IN (
            SELECT
                fact_date
            FROM {mart_schema}.d_calendar
            )
        UNION
        SELECT DISTINCT
            TO_DATE(date_id::TEXT, 'YYYY-MM-DD') AS date_time
        FROM {stage_schema}.customer_research
        WHERE date_id::timestamp WITHOUT TIME ZONE NOT IN (
            SELECT
                fact_date
            FROM {mart_schema}.d_calendar
            )
        ORDER BY date_time
        )
INSERT
INTO
    {mart_schema}.d_calendar(fact_date, day_num, month_num, month_name, year_num)
SELECT DISTINCT
    date_time                            AS fact_date,
    EXTRACT('day' FROM date_time)::int   AS day_num,
    EXTRACT('month' FROM date_time)::int AS month_num,
    TO_CHAR(date_time, 'Month')          AS month_name,
    EXTRACT('year' FROM date_time)::int  AS year_num
FROM all_dates
ORDER BY fact_date;

/*
d_customer
 */
INSERT INTO
    {mart_schema}.d_customer(customer_id, first_name, last_name, city_id)
SELECT DISTINCT
    customer_id,
    first_name,
    last_name,
    MAX(city_id)
FROM {stage_schema}.user_order_log
WHERE 1 = 1
  AND customer_id NOT IN (
    SELECT
        customer_id
    FROM {mart_schema}.d_customer
    )
GROUP BY customer_id, first_name, last_name;

/*
d_item
 */
INSERT INTO
    {mart_schema}.d_item(item_id, item_name)
SELECT DISTINCT
    item_id,
    item_name
FROM {stage_schema}.user_order_log
WHERE 1 = 1
  AND item_id NOT IN (
    SELECT
        item_id
    FROM {mart_schema}.d_item
    );

END TRANSACTION;