CREATE SCHEMA IF NOT EXISTS dbo;

DROP TABLE IF EXISTS dbo.table_product_demand;

CREATE TABLE dbo.table_product_demand (
  shop_id VARCHAR(100),
  date TIMESTAMP,
  product_name VARCHAR(100),
  demand VARCHAR(100)
);

COPY dbo.table_product_demand(shop_id, date, product_name, demand)
FROM '/data/table_product_demand.csv' DELIMITER ',' CSV HEADER;