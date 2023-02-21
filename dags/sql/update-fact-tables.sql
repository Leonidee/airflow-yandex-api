START TRANSACTION;
/*
 f_activity
 */
INSERT INTO
    {mart_schema}.f_activity(action_id, date_id, click_number)
SELECT
    ual.action_id,
    dc.date_id,
    SUM(ual.quantity) AS click_number
FROM
    {stage_schema}.user_activity_log AS ual
    LEFT JOIN {mart_schema}.d_calendar AS dc ON TO_DATE(ual.date_time::TEXT, 'YYYY-MM-DD') = dc.fact_date
WHERE 1 = 1
  AND ual.date_time::timestamp WITHOUT TIME ZONE IN ('{report_date}')
GROUP BY 1, 2;

/*
 f_daily_sales
 */
INSERT INTO
    {mart_schema}.f_daily_sales(date_id, item_id, customer_id, status, price, quantity, payment_amount)
SELECT
    dc.date_id,
    uol.item_id,
    uol.customer_id,
    uol.status,
    AVG(uol.payment_amount / uol.quantity),
    SUM(uol.quantity),
    CASE WHEN uol.status = 'refunded' THEN -(SUM(uol.payment_amount)) ELSE SUM(uol.payment_amount) END AS payment_amount
FROM
    {stage_schema}.user_order_log AS uol
    LEFT JOIN {mart_schema}.d_calendar AS dc ON TO_DATE(uol.date_time::TEXT, 'YYYY-MM-DD') = dc.fact_date
WHERE 1 = 1
  AND uol.date_time::timestamp WITHOUT TIME ZONE IN ('{report_date}')
GROUP BY 1, 2, 3, 4;

END TRANSACTION;