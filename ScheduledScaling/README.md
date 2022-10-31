# Sample: Scheduled Scaling of KDA app w/ Amazon EventBridge and AWS Lambda

This sample illustrates how to scale your KDA Flink application on a schedule using Amazon EventBridge and AWS Lambda. Here's the high level approach:

- Use Amazon EventBridge to call an AWS Lambda function on a schedule.
- In the AWS Lambda function, call `UpdateApplication` on your Kinesis Data Analytics application.

Of course, there's a bit more to it than the above 2 steps. For instance, we have to do proper error handling and ensure that we're not updating an application that is already at the expected parallelism. These details and more are explained in the sections following the section below.

## Deploying the schedule based scaler

Please follow this detailed, step-by-step tutorial on using Amazon EventBridge to schedule an AWS Lambda function based on CRON syntax:

[Tutorial: Schedule AWS Lambda functions using EventBridge](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-run-lambda-schedule.html)

For our purposes, you simply have to alter a couple of key details:

1. Replace the lambda function included in that tutorial with the Python 3 lambda function included in this sample: [kda-scaler-lambda.py](kda-scaler-lambda.py).
2. Ensure that you give your AWS Lambda function permission to call `DescribeApplication` and `UpdateApplication` on your Kinesis Data Analytics application. In addition to any other permissions you might need for your AWS Lambda function, please add the permissions shown below (replacing the placeholders with yours):
```
{
    "Sid": "MyKDAPermissions",
    "Effect": "Allow",
    "Action": [
        "kinesisanalytics:UpdateApplication",
        "kinesisanalytics:DescribeApplication"
    ],
    "Resource": [
        "arn:aws:kinesisanalytics:[my-region]:[my-account-number]:application/[my-kda-app-name]"
    ]
},
```

## Configuring the schedule

Amazon EventBridge supports standard cron expressions as described [here](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-create-rule-schedule.html). In this sample, we assume a scenario where we need to scale up our application to 20 KPUs during the daytime (US Pacific time) and scale down to 10 KPUs during nighttime (US Pacific time).

In order to facilitate this, we could setup up our Amazon EventBridge scheduler to run our AWS Lambda scaler at *5:05AM PST* and *6:05PM PST* every day.

Please keep in mind that this is just one possible scenario. You can use this sample as a starting point and implement more sophisticated scenarios based on your needs. Please see this [article](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-run-lambda-schedule.html) on Amazon EventBridge cron expressions for more details.

## Scaling logic

As alluded to above, the scaling logic is a bit more involved than simply calling `UpdateApplication` on your Kinesis Data Analytics application. Here are the steps involved:

1. Call `DescribeApplication` to get the current version id and application status. The current application version id needs to be supplied as a parameter when calling `UpdateApplication` - for optimistic concurrency reasons.
2. If application status is not RUNNING, then we simply exit.
3. We then check to see if we're currently in a high scale period or a low scale period.
4. Before updating the application's KPU count, we also check to see if the current KPU count is already at the expected value. If so, we simply exit.
5. If we've gotten this far, it means that we need to call `UpdateApplication` with the desired KPU count as well as the current application version.

NOTE: In this sample, we assume that the parallelism/KPU is 1. For more background on parallelism and parallelism/KPU, please see [Application Scaling in Kinesis Data Analytics](https://docs.aws.amazon.com/kinesisanalytics/latest/java/how-scaling.html).

## Error handling

The included Python code catches exceptions and prints an error message. We recommend that you handle errors in a more fine grained fashion based on the guidance here: [error handling in Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html).

## Parametrizing application properties

You might consider parametrizing key application variables in your AWS Lambda function such as the region and the name of the application being scaled using AWS Lambda environment variables. Please see [Using AWS Lambda environment variables](https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html) for more details.

## Pricing

Please keep in mind that there are costs associated with the key components used in this sample:

- [Amazon EventBridge pricing](https://aws.amazon.com/eventbridge/pricing/).
- [Amazon Lambda pricing](https://aws.amazon.com/lambda/pricing/).

## Troubleshooting

1. Access denied exception of the form: `An error occurred (AccessDeniedException) when calling the DescribeApplication operation`.

    Please ensure that you've given the role associated with your AWS Lambda function permissions to call `DescribeApplication` and `UpdateApplication` on your Kinesis Data Analytics application.

2. Kinesis Data Analytics application is not scaled unless it is running.

    This is by design. The included Python code first checks to see if the Kinesis Data Analytics application is running and only updates the KPU count if the application has a status of RUNNING.

## References

- [Amazon Kinesis Data Analytics developer guide](https://docs.aws.amazon.com/kinesisanalytics/latest/java/what-is.html).
- [Application Scaling in Kinesis Data Analytics](https://docs.aws.amazon.com/kinesisanalytics/latest/java/how-scaling.html).
- [KinesisAnalyticsV2 boto3 reference](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesisanalyticsv2.html).
- [Amazon EventBridge developer guide](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-what-is.html).
- [Boto3 error handling](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html).