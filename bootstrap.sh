#!/bin/bash
aws s3 cp s3://spark-bucket-aakash/configs/log4j.properties /tmp/log4j.properties
mkdir -p /tmp/logs
