CREATE DATABASE IF NOT EXISTS warehouse;
\c warehouse;

CREATE TABLE IF NOT EXISTS sales_events (
  ts timestamptz,
  store_id int,
  product_id text,
  sales int,
  inventory int,
  restock int
);

CREATE TABLE IF NOT EXISTS sales_daily (
  date date,
  store_id int,
  product_id text,
  qty_sold int,
  avg_price numeric NULL,
  inventory_end int,
  PRIMARY KEY (date, store_id, product_id)
);
