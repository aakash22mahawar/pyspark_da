#!/bin/bash

# Step 1: Download custom log4j.properties from S3
aws s3 cp s3://spark-bucket-aakash/configs/log4j.properties /tmp/log4j.properties

# Step 2: Create logs directory
mkdir -p /tmp/logs

# Step 3: OPTIONAL — export SPARK_LOG_DIR if used in your spark-submit
export SPARK_LOG_DIR=/tmp/logs

# --- Your Spark job will run here and generate /tmp/logs/spark_app.log ---

# Step 4: Upload generated Spark log file to S3
# (This line ensures that even if the job ends later, logs are preserved)
aws s3 cp /tmp/logs/spark_app.log s3://spark-bucket-aakash/spark_logs/spark_app-$(date +%d%m%Y%H%M%S).log
