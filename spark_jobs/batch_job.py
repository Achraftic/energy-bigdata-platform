from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Spark and Cassandra configuration
SPARK_MASTER = "spark://spark-master:7077"
HDFS_INPUT_PATH = "hdfs://namenode:9000/user/root/energy_data.csv"
CASSANDRA_HOST = "cassandra"
CASSANDRA_KEYSPACE = "energy_analysis"
CASSANDRA_TABLE = "energy_data"


def main():
    spark = (
        SparkSession.builder.appName("EnergyBatchProcessing")
        .master(SPARK_MASTER)
        .config(
            "spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0",
        )
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .getOrCreate()
    )

    # Read data from HDFS
    df = spark.read.csv(HDFS_INPUT_PATH, header=True, inferSchema=True)

    # Invert/Cast columns if necessary to match Cassandra
    from pyspark.sql.functions import to_timestamp
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

    # Select columns to match Cassandra table
    final_df = df.select(
        "timestamp", "region", "power_plant", "consumer_type",
        "production", "consumption", "voltage", "frequency", "equipment_status"
    )

    # Write data to Cassandra
    final_df.write.format("org.apache.spark.sql.cassandra").options(
        table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE
    ).mode("append").save()

    print("Batch job completed successfully!")


if __name__ == "__main__":
    main()
