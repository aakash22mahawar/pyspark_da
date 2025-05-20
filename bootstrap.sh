#!/bin/bash

echo "Bootstrap script started"

# Download log4j config
aws s3 cp s3://spark-bucket-aakash/configs/log4j.properties /tmp/log4j.properties
echo "Downloaded log4j.properties"

# Create logs directory
mkdir -p /tmp/logs
echo "Created /tmp/logs directory"

# List /tmp contents and log them
echo "=== /tmp directory contents ===" >> /tmp/bootstrap_log.txt
ls -lh /tmp >> /tmp/bootstrap_log.txt

# Print log to stdout so EMR uploads it
cat /tmp/bootstrap_log.txt

echo "Bootstrap script completed"
