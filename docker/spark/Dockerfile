FROM apache/spark:4.0.2-python3

USER root

RUN curl -fsSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
  -o /opt/spark/jars/postgresql-42.7.3.jar \
  && curl -fsSL https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3-all.jar \
  -o /opt/spark/jars/clickhouse-jdbc-0.6.3-all.jar

RUN python3 -m pip install --no-cache-dir \
  "pydantic>=2.13.3,<3.0.0" \
  "pydantic-settings>=2.14.0,<3.0.0" \
  "psycopg2-binary>=2.9.12,<3.0.0"

WORKDIR /opt/project
