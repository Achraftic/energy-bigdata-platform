from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler


# Spark, Cassandra and Model configuration
SPARK_MASTER = "spark://spark-master:7077"
CASSANDRA_HOST = "cassandra"
CASSANDRA_KEYSPACE = "energy_analysis"
INPUT_TABLE = "energy_data"
OUTPUT_TABLE = "power_forecast"
MODEL_PATH = "/opt/spark/apps/ml_models/forecasting_model"


def main():
    spark = (
        SparkSession.builder.appName("EnergyForecasting")
        .master(SPARK_MASTER)
        .config(
            "spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0",
        )
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Load the trained model
    model = LinearRegressionModel.load(MODEL_PATH)

    # Read new data from Cassandra
    df = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table=INPUT_TABLE, keyspace=CASSANDRA_KEYSPACE)
        .load()
    )

    # Feature engineering - use multiple features for better predictions
    assembler = VectorAssembler(
        inputCols=["production", "voltage", "frequency"], outputCol="features"
    )
    data = assembler.transform(df)

    # Make predictions
    predictions = model.transform(data)

    # Rename prediction column
    predictions = predictions.withColumnRenamed("prediction", "forecasted_consumption")

    # Calculate forecasted_production based on forecasted consumption ratio
    predictions = predictions.withColumn(
        "forecasted_production",
        col("forecasted_consumption")
        * (col("production") / (col("consumption") + 0.001)),
    )

    # Write predictions to Cassandra
    (
        predictions.select(
            "timestamp", "region", "forecasted_production", "forecasted_consumption"
        )
        .write.format("org.apache.spark.sql.cassandra")
        .options(table=OUTPUT_TABLE, keyspace=CASSANDRA_KEYSPACE)
        .mode("append")
        .save()
    )

    print("Forecasting job completed successfully!")


if __name__ == "__main__":
    main()
