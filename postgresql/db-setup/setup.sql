CREATE SCHEMA IF NOT EXISTS dbo;

DROP TABLE IF EXISTS dbo.table_product_demand;

CREATE TABLE dbo.table_product_demand (
  date TIMESTAMP,
  shop_id VARCHAR(100),
  product_name VARCHAR(100),
  demand VARCHAR(100)
);

COPY dbo.table_product_demand(date, shop_id, product_name)
FROM '/data/table_product_demand.csv' DELIMITER ',' CSV HEADER;