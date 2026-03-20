#!/bin/bash
echo "Starting HDFS data loader..."
until hdfs dfs -ls /; do
  echo "Waiting for NameNode to be ready..."
  sleep 5
done

echo "Creating /user/root directory..."
hdfs dfs -mkdir -p /user/root

echo "Uploading energy_data.csv to HDFS..."
hdfs dfs -put -f /energy_data.csv /user/root/energy_data.csv

echo "Upload complete!"
hdfs dfs -ls /user/root/
