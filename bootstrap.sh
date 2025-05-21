#!/bin/bash

echo "Bootstrap script started"

# Download log4j config
aws s3 cp s3://spark-bucket-aakash/configs/log4j.properties /tmp/log4j.properties
echo "Downloaded log4j.properties"

# Create logs directory
mkdir -p /tmp/logs
echo "Created /tmp/logs directory"

echo "Bootstrap script completed"

# âœ… Upload actual EMR bootstrap logs to S3
timestamp=$(date +%d-%m-%y-%H-%M-%S)
aws s3 cp --recursive /emr/instance-controller/log/bootstrap-actions/ \
  s3://spark-bucket-aakash/spark_logs/real_bootstrap_logs/${timestamp}/
echo "Uploaded actual EMR bootstrap logs to S3 at spark_logs/real_bootstrap_logs/${timestamp}/"
