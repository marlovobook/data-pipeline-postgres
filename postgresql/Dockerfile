FROM postgres

COPY db-setup/*.sql /docker-entrypoint-initdb.d/

ADD db-setup/setup.sql /docker-entrypoint-initdb.d

COPY data/* /data/

ADD data/table_product_demand.csv /data

RUN chmod a+r /docker-entrypoint-initdb.d/*

# FROM quay.io/astronomer/astro-runtime:8.2.0
# RUN pip install astronomer-providers