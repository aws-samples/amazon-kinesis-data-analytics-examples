# CloudWatch Metrics Async Sink Example

This sample project demonstrates how to build a simple Sink using the Async Sink framework.

The example application uses two dummy data generator sources and delivers metrics to Amazon CloudWatch Metrics. 
Two hours of bulk data is generated when the job starts up, followed by one record per second per data source.

## Flink Compatibility

**Note:** This project is compatible with Apache Flink 1.15+. 
This project is not compatible with Amazon Kinesis Data Analytics until Apache Flink 1.15 or highter is supported.

