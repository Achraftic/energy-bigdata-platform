FROM apache/spark:3.5.0-python3

WORKDIR /opt/spark/apps

COPY spark_jobs/requirements.txt .

USER root
RUN pip install --no-cache-dir --default-timeout=100 -r requirements.txt

# Add user 1001 to /etc/passwd and create home
RUN useradd -m -u 1001 -g 0 sparkuser || true

USER 1001
ENV HOME=/home/sparkuser
# Pre-download jars as sparkuser
RUN /opt/spark/bin/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 --version

WORKDIR /opt/spark/apps
