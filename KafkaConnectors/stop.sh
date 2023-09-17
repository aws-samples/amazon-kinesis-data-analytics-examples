#!/bin/bash
set -eo pipefail
stack_name=flink-application
application_name=flink-app-cft
region=ap-south-1
bootstrap_server="boot-z6eo0mfk.c1.kafka-serverless.ap-south-1.amazonaws.com:9098"


aws kinesisanalyticsv2 stop-application --application-name ${application_name} --region ${region}

