-- CREATE DATABASE IF NOT EXISTS warehouse;
-- \c warehouse;

-- CREATE TABLE IF NOT EXISTS sales_events (
--   ts timestamptz,
--   store_id int,
--   product_id text,
--   sales int,
--   inventory int,
--   restock int
-- );

-- CREATE TABLE IF NOT EXISTS sales_daily (
--   date date,
--   store_id int,
--   product_id text,
--   qty_sold int,
--   avg_price numeric NULL,
--   inventory_end int,
--   PRIMARY KEY (date, store_id, product_id)
-- );

-- Create schema for data warehouse
-- CREATE DATABASE warehouse;
CREATE SCHEMA IF NOT EXISTS warehouse;
\c warehouse;

CREATE TABLE IF NOT EXISTS warehouse.sales (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP NOT NULL,
    store_id VARCHAR(50),
    product_id VARCHAR(50),
    category VARCHAR(100),
    quantity_sold INT,
    unit_price NUMERIC(10,2),
    discount_percent NUMERIC(5,2),
    revenue NUMERIC(12,2),
    cost NUMERIC(12,2),
    profit NUMERIC(12,2)
);
