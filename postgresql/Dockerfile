FROM postgres:13

COPY db-setup/*.sql /docker-entrypoint-initdb.d/

ADD db-setup/setup.sql /docker-entrypoint-initdb.d

COPY data/* /data/

ADD data/table_product_demand.csv /data

RUN chmod a+r /docker-entrypoint-initdb.d/*