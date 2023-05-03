# Apache Flink StateFun Playground Example for Amazon Kinesis Data Analytics

This repository has been extended from the Apache Flink StateFun [playground](https://github.com/apache/flink-statefun-playground) project skeleton.

It contains the greeter example for how to deploy Stateful Functions on AWS Lambda and Amazon Kinesis Data Analytics or running the Flink StateFun with docker compose on your local computer.

This example container the following directories:

```
- Java
- python
```

**- Java:** - Contains the Apache Flink StateFun runtime. You need to compile this application and deploy the JAR file on [Kinesis Analytics](https://docs.aws.amazon.com/kinesisanalytics/latest/java/how-creating-apps.html#how-creating-apps-creating). This Flink application is generic and does not need to be changed. All the required configuration is managed through module.yaml. You can build the application with `mvn clean package`. This application only runs on Kinesis Data Analytics for Flink Version 1.13. For testing the recovery scenario, stop the Kinesis Analytics application after processing a few greeting messages. Start the application with the latest Snapshot. Send another name to `greeter-ingress` topic in Apache Kafka. You should see StateFun remembers all previous visits of this person. 

**- Python** - Contains the python code for two AWS Lambda functions. These Lambda functions are invoked by Apache Flink StateFun runtime via AWS API Gateway (HTTPs) end-points. Since the operators are remote you can change the code for enhancements or bug fixes with zero down-time. 