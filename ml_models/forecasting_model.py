from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession

# Spark and Cassandra configuration
SPARK_MASTER = "spark://spark-master:7077"
CASSANDRA_HOST = "cassandra"
CASSANDRA_KEYSPACE = "energy_analysis"
CASSANDRA_TABLE = "energy_data"
MODEL_PATH = "/app/ml_models/forecasting_model"


def main():
    spark = (
        SparkSession.builder.appName("EnergyForecastingTraining")
        .master(SPARK_MASTER)
        .config(
            "spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0",
        )
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Read data from Cassandra
    df = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table=CASSANDRA_TABLE, keyspace=CASSANDRA_KEYSPACE)
        .load()
    )

    # Feature engineering
    assembler = VectorAssembler(
        inputCols=["production", "voltage", "frequency"], outputCol="features"
    )
    data = assembler.transform(df)

    # Train a linear regression model
    lr = LinearRegression(featuresCol="features", labelCol="consumption")
    model = lr.fit(data)

    # Save the model
    model.write().overwrite().save(MODEL_PATH)

    print("Forecasting model trained and saved successfully!")


if __name__ == "__main__":
    main()
