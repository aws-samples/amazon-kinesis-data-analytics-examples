#!/bin/bash
set -eo pipefail
stack_name=flink-application
application_name=flink-app-cft
region=ap-south-1
bootstrap_server="boot-z6eo0mfk.c1.kafka-serverless.ap-south-1.amazonaws.com:9098"


echo "Deploying Provisioned"
aws cloudformation deploy --template-file cloudformation.yaml --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
     --region ${region} \
     --stack-name ${stack_name}  \
     --parameter-overrides ApplicationName=${application_name} \
     FlinkRuntimeEnvironment=FLINK-1_15 \
     CodeBucketArn="arn:aws:s3:::aksh-code-binaries" \
     CodeKey=flink/KafkaGettingStartedJob-1.0.jar \
     KafkaBootstrapserver=${bootstrap_server}


aws kinesisanalyticsv2 start-application --application-name ${application_name} --region ${region}

