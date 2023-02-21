START TRANSACTION;
/*
 d_calendar
 */
WITH
    all_dates AS (
        SELECT DISTINCT
            TO_DATE(date_time::TEXT, 'YYYY-MM-DD') AS date_time
        FROM stage.user_activity_log
        UNION
        SELECT DISTINCT
            TO_DATE(date_time::TEXT, 'YYYY-MM-DD') AS date_time
        FROM stage.user_order_log
        UNION
        SELECT DISTINCT
            TO_DATE(date_id::TEXT, 'YYYY-MM-DD') AS date_time
        FROM stage.customer_research
        ORDER BY date_time
        )
INSERT
INTO
    mart.d_calendar(fact_date, day_num, month_num, month_name, year_num)
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
    mart.d_customer (customer_id, first_name, last_name, city_id)
SELECT DISTINCT
    customer_id,
    first_name,
    last_name,
    MAX(city_id)
FROM stage.user_order_log
GROUP BY customer_id, first_name, last_name;

/*
d_item
 */
INSERT INTO
    mart.d_item(item_id, item_name)
SELECT
    item_id,
    last_item_name AS item_name
FROM
    (
        SELECT DISTINCT
            item_id,
            item_name,
            LAST_VALUE(item_name)
            OVER (PARTITION BY item_id ORDER BY date_time ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS last_item_name
        FROM stage.user_order_log
        ORDER BY item_id
        ) q
GROUP BY 1, 2
ORDER BY 1;

/*
 f_activity
 */
INSERT INTO
    mart.f_activity(action_id, date_id, click_number)
SELECT
    ual.action_id,
    dc.date_id,
    SUM(ual.quantity) AS click_number
FROM
    stage.user_activity_log AS ual
    LEFT JOIN mart.d_calendar AS dc ON TO_DATE(ual.date_time::TEXT, 'YYYY-MM-DD') = dc.fact_date
GROUP BY 1, 2;

/*
 f_daily_sales
 */
INSERT INTO
    mart.f_daily_sales(date_id, item_id, customer_id, price, quantity, payment_amount, status)
SELECT
    dc.date_id,
    uol.item_id,
    uol.customer_id,
    AVG(uol.payment_amount / uol.quantity),
    SUM(uol.quantity),
    SUM(uol.payment_amount),
    'shipped' as status
FROM
    stage.user_order_log AS uol
    LEFT JOIN mart.d_calendar AS dc ON TO_DATE(uol.date_time::TEXT, 'YYYY-MM-DD') = dc.fact_date
GROUP BY 1, 2, 3;

END TRANSACTION;