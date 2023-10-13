# Apache Flink Stateful Functions on Docker

This example contains an adaptation of the [StateFun Python example](https://github.com/apache/flink-statefun-playground/tree/release-3.1) for Amazon Kinesis Data Analytics.

The StateFun runtime application reads from a topic in Apache Kafka (greeter-ingress), invokes two AWS Lambda functions (person & greeter) through HTTPs API Gateway, then sends the result to another Kafka topic (greeter-egress).

First you need to deploy the AWS Lambda functions. To create the infrastructure, execute the CDK template by running `cdk deploy` in the `cdk` folder. It will create the data streams, the [Lambda functions](cdk/lambda/index.py), and the API Gateway.

The most important aspect in addition to the Lambda functions is the [module.yaml](src/main/resources/module.yaml) file. It specifies the resources that are available to the StateFun application and how they interact with each other. You need to adapt the API Gateway endpoint in the `module.yaml` file to match the one that has been created by the CDK template.

If you wish to run Flink StateFun runtime locally, you need to build and run Apache Flink Statefun on Docker. You can do that on your computer or on a remote machine by running `docker compose build` and then `docker compose up`. Alternatively you can compile and deploy Apache Flink StateFun runtime on Amazon Kinesis Data Analytics for Flink V1.13. Use the guide on Kinesis Analytics [Example page](https://github.com/awsalialem/amazon-kinesis-data-analytics-java-examples/tree/add-flink-state-fun/FlinkStateFun) for more information.


To generate the input messages and view the output messages we are running `Examples.ipynb` Apache Zeppelin notebook on [Amazon Kinesis Analytics Studio](https://aws.amazon.com/blogs/aws/introducing-amazon-kinesis-data-analytics-studio-quickly-interact-with-streaming-data-using-sql-python-or-scala/).

## Play around!

Open the `greeter` AWS Lambda function in the AWS console. Try to modify `compute_fancy_greeting` function in how it generates greeting messages. Click on `Deploy` button. You should see the changes immediately in the output. 

Now, let's test the recovery of Apache Flink Statefun. Using the Apache Flink's Job Manager REST endpoint, let's stop the Flink StateFun cluster after triggering a [savepoint](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/savepoints/).

```
$ curl -X POST -H "Content-Type: application/json" \
    --data '"{\"target-directory\":\"file:///savepoint-dir/\",\"cancel-job\":true}"' \
    http://localhost:8081/jobs/:jobId/savepoints
```

Now, list all files within `savepoint-dir`. Copy its name and replace `{savepoint-DIR}` with it, in the `docker-compose.yml` file. Run `docker compose build` and `docker compose run` again to start a new job. 

Send another message with a name that you have used in previous messages. You should see the Job remembers this name and generates an appropriate greeting message. 