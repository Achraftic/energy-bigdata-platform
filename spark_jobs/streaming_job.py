from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit, concat
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Spark, Kafka, and Cassandra configuration
SPARK_MASTER = "spark://spark-master:7077"
KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "energy_topic"
CASSANDRA_HOST = "cassandra"
CASSANDRA_KEYSPACE = "energy_analysis"
ANOMALY_TABLE = "anomaly_data"
ENERGY_TABLE = "energy_data"


def main():
    spark = (
        SparkSession.builder.appName("EnergyStreamingProcessing")
        .master(SPARK_MASTER)
        .config(
            "spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Define schema for Kafka data
    schema = StructType(
        [
            StructField("timestamp", StringType(), True),
            StructField("region", StringType(), True),
            StructField("power_plant", StringType(), True),
            StructField("consumer_type", StringType(), True),
            StructField("production", DoubleType(), True),
            StructField("consumption", DoubleType(), True),
            StructField("voltage", DoubleType(), True),
            StructField("frequency", DoubleType(), True),
            StructField("equipment_status", StringType(), True),
        ]
    )

    # Read data from Kafka
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .load()
    )

    # Deserialize JSON data
    json_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    json_df = json_df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

    # Write raw data to Cassandra
    query_raw = (
        json_df.select(
            "timestamp", "region", "power_plant", "consumer_type",
            "production", "consumption", "voltage", "frequency", "equipment_status"
        )
        .writeStream.outputMode("append")
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", CASSANDRA_KEYSPACE)
        .option("table", ENERGY_TABLE)
        .option("checkpointLocation", "/tmp/checkpoint_raw")
        .start()
    )

    # Anomaly detection (simple statistical model)
    # A more advanced model like Isolation Forest would require more setup
    # and potentially a separate ML model training pipeline.
    # For this example, we'll flag significant deviations from a baseline.

    # Define a simple anomaly detection logic
    # For example, an anomaly is when production is 50% higher than consumption
    # or equipment_status is not 'ok'
    anomaly_df = (
        json_df.filter(
            (col("production") > col("consumption") * 1.5)
            | (col("equipment_status") != "ok")
        )
        .withColumn("anomaly_type", when(col("production") > col("consumption") * 1.5, "overproduction").otherwise("equipment_fault"))
        .withColumn("description", concat(lit("Status: "), col("equipment_status")))
    )

    # Write anomalies to Cassandra
    query_anomaly = (
        anomaly_df.select("timestamp", "region", "anomaly_type", "description")
        .writeStream.outputMode("append")
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", CASSANDRA_KEYSPACE)
        .option("table", ANOMALY_TABLE)
        .option("checkpointLocation", "/tmp/checkpoint_anomaly")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
