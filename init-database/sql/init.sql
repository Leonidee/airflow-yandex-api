START TRANSACTION;
/*
 Create schemas
 */
DROP SCHEMA IF EXISTS mart CASCADE;
DROP SCHEMA IF EXISTS stage CASCADE;

CREATE SCHEMA mart;
CREATE SCHEMA stage;

/*
 Create tables on stage schema
 */
CREATE TABLE stage.customer_research
(
    date_id     timestamp WITHOUT TIME ZONE,
    category_id bigint,
    geo_id      integer,
    sales_qty   integer,
    sales_amt   bigint
);

CREATE TABLE stage.user_activity_log
(
    date_time   timestamp WITHOUT TIME ZONE,
    action_id   int,
    customer_id bigint,
    quantity    bigint
);

CREATE TABLE stage.user_order_log
(
    date_time      timestamp WITHOUT TIME ZONE,
    city_id        int,
    city_name      varchar(100),
    customer_id    bigint,
    first_name     varchar(100),
    last_name      varchar(100),
    item_id        int,
    item_name      varchar(150),
    quantity       integer,
    payment_amount numeric(14, 2),
    status         varchar(50)
);

/*
 Creating tables in mart schema
 */

CREATE TABLE mart.d_calendar
(
    date_id    serial PRIMARY KEY,
    fact_date  timestamp WITHOUT TIME ZONE,
    day_num    int,
    month_num  int,
    month_name varchar(100),
    year_num   int
);

CREATE TABLE mart.d_customer
(
    customer_id bigint,
    first_name  varchar(100),
    last_name   varchar(100),
    city_id     bigint
);

CREATE TABLE mart.d_item
(
    item_id   bigint,
    item_name varchar(150)
);

CREATE TABLE mart.f_daily_sales
(
    date_id        bigint,
    item_id        bigint,
    customer_id    bigint,
    price          decimal(14, 2),
    quantity       bigint,
    payment_amount decimal(14, 2),
    status         varchar(50),

    CONSTRAINT fk_date_id_daily_sales
        FOREIGN KEY (date_id)
            REFERENCES mart.d_calendar (date_id) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE mart.f_activity
(
    action_id    bigint,
    date_id      bigint,
    click_number bigint,

    CONSTRAINT fk_date_id_f_activity
        FOREIGN KEY (date_id)
            REFERENCES mart.d_calendar (date_id) ON UPDATE CASCADE ON DELETE CASCADE
);

END TRANSACTION;