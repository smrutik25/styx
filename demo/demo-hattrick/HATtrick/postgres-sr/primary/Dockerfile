FROM postgres:14.5
COPY ./postgres-sr/primary/init.sql /docker-entrypoint-initdb.d/
COPY ./postgres-sr/primary/pg_hba.conf /etc/postgresql/pg_hba.conf
COPY ./datagen /data/datagen
