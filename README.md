# End-to-End Big Data Platform for National Energy Data Analysis

This project is a complete end-to-end Big Data platform for national energy data analysis. The system is production-like, modular, and runnable locally with Docker. It uses fake/generated data only.

## Architecture

```
+------------------+      +-----------------+      +-----------------+
| Data Generator   |----->| Kafka Producer  |----->|      Kafka      |
+------------------+      +-----------------+      +-----------------+
        |                                                 |
        |                                                 |
        v                                                 v
+------------------+      +-----------------+      +-----------------+
|   HDFS Datanode  |<-----|  HDFS Namenode  |      |  Spark Streaming|
+------------------+      +-----------------+      +-----------------+
        ^                                                 |
        |                                                 |
        |                                                 v
+------------------+      +-----------------+      +-----------------+
|   Spark Batch    |----->|    Cassandra    |<-----|   Spark MLlib   |
+------------------+      +-----------------+      +-----------------+
                                    |
                                    |
                                    v
+------------------+      +-----------------+
|   FastAPI Backend  |----->|Streamlit Dashboard|
+------------------+      +-----------------+

```

## Tech Stack

*   **Data Generation**: Python (Faker / custom generator)
*   **Streaming**: Apache Kafka
*   **Storage (raw)**: Hadoop HDFS
*   **Processing**: Apache Spark (PySpark + Spark MLlib)
*   **NoSQL Analytics DB**: Cassandra
*   **Backend API**: FastAPI
*   **Visualization**: Streamlit
*   **Orchestration**: Docker Compose

## Project Structure

```
energy-bigdata-platform/
│── data_generator/
│── kafka_producer/
│── spark_jobs/
│── ml_models/
│── api/
│── dashboard/
│── docker/
│── configs/
│── README.md
```

## Setup and Running the Platform

1.  **Prerequisites**:
    *   Docker
    *   Docker Compose

2.  **Build and Run**:
    Open a terminal in the root of the project and run:

    ```bash
    docker compose up -d --build
    ```

    > [!NOTE]
    > The first build will take a few minutes as it pre-downloads Spark dependencies (jars) to ensure stable offline execution.

3.  **Verify the Pipeline**:
    After starting, you can verify that data is flowing:
    - **Check Data Generation**: Verify `data_generator/energy_data.csv` has been created and contains data.
    - **Check Kafka**: Verify messages are being sent:
      ```bash
      docker compose logs -f kafka-producer
      ```
    - **Check Spark**: Monitor real-time processing:
      ```bash
      docker compose logs -f spark-streaming
      ```
    - **Check Database**: Verify data is landing in Cassandra:
      ```bash
      docker exec cassandra cqlsh -e "SELECT count(*) FROM energy_analysis.energy_data;"
      ```

4.  **Accessing the Services**:
    *   **FastAPI Backend**: [http://localhost:8000](http://localhost:8000)
    *   **Streamlit Dashboard**: [http://localhost:8501](http://localhost:8501)
    *   **Spark Master UI**: [http://localhost:8080](http://localhost:8080)
    *   **HDFS Namenode UI**: [http://localhost:9870](http://localhost:9870)

## How Each Component Works

*   **Data Generator**: The `data_generator` service generates synthetic energy data and saves it to a CSV file. This data is used by the `hdfs-data-loader` and the `kafka-producer`.
*   **Kafka Producer**: The `kafka_producer` service reads the synthetic data from the `data_generator` and sends it to the `energy_topic` in Kafka.
*   **HDFS Data Loader**: The `hdfs-data-loader` service loads the batch data from the CSV file into HDFS.
*   **Spark Jobs**:
    *   `batch_job.py`: This Spark job reads the batch data from HDFS, performs aggregations, and writes the results to Cassandra.
    *   `streaming_job.py`: This Spark job reads the real-time data from Kafka, performs anomaly detection, and writes the results to Cassandra.
    *   `forecasting_model.py`: This script trains a linear regression model on the data in Cassandra and saves the model.
    *   `forecasting_job.py`: This Spark job uses the trained model to make predictions and saves them to Cassandra.
*   **Cassandra**: The Cassandra database stores the raw data, anomaly data, and power forecast data.
*   **FastAPI Backend**: The FastAPI application provides a REST API to access the data from Cassandra.
*   **Streamlit Dashboard**: The Streamlit dashboard visualizes the data from the FastAPI backend.

## Example Queries

You can query the data in Cassandra using `cqlsh`.

1.  **Connect to the Cassandra container**:

    ```bash
    docker exec -it cassandra cqlsh
    ```

2.  **Query the data**:

    ```sql
    USE energy_analysis;

    SELECT * FROM energy_data LIMIT 10;
    SELECT * FROM anomaly_data LIMIT 10;
    SELECT * FROM power_forecast LIMIT 10;
    ```

## Screenshots

(You can add screenshots of the Streamlit dashboard here)
