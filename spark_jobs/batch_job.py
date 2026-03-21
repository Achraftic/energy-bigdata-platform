import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Spark and Cassandra configuration
SPARK_MASTER = "spark://spark-master:7077"
HDFS_INPUT_PATH = "hdfs://namenode:9000/user/root/energy_data.csv"
CASSANDRA_HOST = "cassandra"
CASSANDRA_KEYSPACE = "energy_analysis"
CASSANDRA_TABLE = "energy_data"


def main():
    logger.info("Starting Energy Batch Processing Job")
    
    try:
        spark = (
            SparkSession.builder.appName("EnergyBatchProcessing")
            .master(SPARK_MASTER)
            .config("spark.cassandra.connection.host", CASSANDRA_HOST)
            # Resource management
            .config("spark.executor.memory", "512m")
            .config("spark.cores.max", "1")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark session created successfully")

        # Wait for data to be available in HDFS (max 60s)
        logger.info(f"Waiting for data in HDFS: {HDFS_INPUT_PATH}")
        max_retries = 12
        file_found = False
        for i in range(max_retries):
            try:
                # Use spark.read.csv with a small limit or just check path via JVM if possible
                # Simple check: try to read schema
                spark.read.csv(HDFS_INPUT_PATH, header=True)
                file_found = True
                logger.info("Data file detected in HDFS.")
                break
            except Exception:
                logger.warning(f"Data not ready yet (attempt {i+1}/{max_retries})...")
                time.sleep(5)
        
        if not file_found:
            logger.error("Data file not found or inaccessible in HDFS after timeout!")
            sys.exit(1)

        # Read data from HDFS
        logger.info(f"Reading data from HDFS: {HDFS_INPUT_PATH}")
        df = spark.read.csv(HDFS_INPUT_PATH, header=True, inferSchema=True)
        
        count = df.count()
        if count == 0:
            logger.warning("No data found in HDFS path!")
            return
        
        logger.info(f"Successfully loaded {count} rows from HDFS")

        # Invert/Cast columns if necessary to match Cassandra
        logger.info("Processing data and casting timestamp")
        df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

        # Select columns to match Cassandra table
        final_df = df.select(
            "timestamp", "region", "power_plant", "consumer_type",
            "production", "consumption", "voltage", "frequency", "equipment_status"
        )

        # Write data to Cassandra
        logger.info(f"Writing data to Cassandra table: {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}")
        final_df.write.format("org.apache.spark.sql.cassandra").options(
            table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE
        ).mode("append").save()

        logger.info("Batch job completed successfully!")
        
    except Exception as e:
        logger.error(f"Batch job failed with error: {str(e)}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
