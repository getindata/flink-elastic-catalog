# example

This directory contains definition docker-compose environment which makes it possible to use Jupyter Notebook with the
Flink JDBC Elastic Catalog.

The docker-compose environment consists of:

- a standalone Flink cluster (`jobmanager` and `taskmanager` containers).
- an "edge" node from which Flink jobs can be scheduled (`flink-sql-runner-node` container).
- `MinIO` instance which serves as AWS S3 substitute.
- a short-lived container `createbuckets` which configures minio, namely, creates necessary buckets, policies and
  users.
- Elastic instance.
- a short-lived container `elastic-data` which puts test data into Elastic.
- a Jupyter Notebook container.

Flink UI can be accessed at `http://localhost:8081`.
Minio UI can be accessed at `http://localhost:9001` (`user`/`password`).
Jupyter UI can be accessed at `http://localhost:8888`.

## Build

1. Build java artifacts first (`flink-connector-jdbc` fork and `flink-elastic-catalog`). Run the commands below from the
   project root directory.
   ```bash
   mvn clean install -DskipTests -T 1C -f flink-connector-jdbc/pom.xml
   mvn clean install -DskipTests -T 1C
   ```
2. Build and start docker environment.
   ```bash
   cd example/docker
   docker compose build
   docker compose up
   ```

## Run ad-hoc batch query on Jupyter Notebook

Open Jupyter Notebooks UI on your local browser `http://localhost:8888`, open `elastic-catalog-example-notebook.ipynb`
notebook and run the cells one by one. The notebook shows how to explore elastic schema and run SQL queries.
