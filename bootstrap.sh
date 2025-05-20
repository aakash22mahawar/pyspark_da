#!/bin/bash

echo "Bootstrap script started"

aws s3 cp s3://spark-bucket-aakash/configs/log4j.properties /tmp/log4j.properties
echo "Downloaded log4j.properties"

mkdir -p /tmp/logs
echo "Created /tmp/logs directory"

echo "Bootstrap script completed"
