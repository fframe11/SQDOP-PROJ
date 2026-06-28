CREATE TABLE IF NOT EXISTS sales_records (
    region VARCHAR(100),
    country VARCHAR(100),
    item_type VARCHAR(100),
    sales_channel VARCHAR(50),
    order_priority VARCHAR(10),
    order_date VARCHAR(50),
    order_id BIGINT PRIMARY KEY,
    ship_date VARCHAR(50),
    units_sold INTEGER,
    unit_price NUMERIC(10,2),
    unit_cost NUMERIC(10,2),
    total_revenue NUMERIC(15,2),
    total_cost NUMERIC(15,2),
    total_profit NUMERIC(15,2)
);

TRUNCATE TABLE sales_records;

COPY sales_records(region, country, item_type, sales_channel, order_priority, order_date, order_id, ship_date, units_sold, unit_price, unit_cost, total_revenue, total_cost, total_profit)
FROM '/tmp/sales_records.csv'
DELIMITER ','
CSV HEADER;
