import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as path from 'path';
import {CfnParameter} from "aws-cdk-lib";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";


export class ScheduledScalingCdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const kdaAppName = new CfnParameter(this, "kdaAppName", {
      type: "String",
      description: "The name of the Kinesis Data Analytics Application that you want to scale based on schedule."});

    const scaleUpHour = new CfnParameter(this, "scaleUpHour", {
      type: "Number",
      description: "Hour of the day when you want your KDA Application to Scale Up"});

    const scaleDownHour = new CfnParameter(this, "scaleDownHour", {
      type: "Number",
      description: "Hour of the day when you want your KDA Application to Scale down"});

    const lowKPU = new CfnParameter(this, "lowKPU", {
      type: "Number",
      description: "KPU you want for your KDA application during normal hours"});

    const highKPU = new CfnParameter(this, "highKPU", {
      type: "Number",
      description: "KPU you want for your KDA application during peak hours"});

    // our KDA app needs access to describe kinesisanalytics
    const kdaAccessPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ['*'],
          actions: ['kinesisanalytics:DescribeApplication','kinesisanalytics:UpdateApplication']
        }),
      ],
    });
    const lambdaRole = new iam.Role(this, 'lambda-scheduler-role', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      description: 'Lambda Scheduler role',
      inlinePolicies: {
        KdaAccessPolicy: kdaAccessPolicy,
      },
    });

    const fn = new lambda.Function(this, 'kda-scheduled-scaler', {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'kda-scaler-lambda.handler',
      code: lambda.Code.fromAsset('../kda-scaler-lambda/'),
      role: lambdaRole,
      environment: {
        "REGION": this.region,
        "KDA_APP_NAME": kdaAppName.valueAsString,
        'SCALE_UP_HOUR': scaleUpHour.valueAsString,
        'SCALE_DOWN_HOUR': scaleDownHour.valueAsString,
        'LOW_KPU': lowKPU.valueAsString,
        'HIGH_KPU': highKPU.valueAsString,
      }
    });

    const ScalingUpRule=new events.Rule(this, 'ScalingUpRule', {
      schedule: events.Schedule.cron({minute:"0", hour:scaleUpHour.valueAsString})
    });


    // add the Lambda function as a target for the Event Rule
    ScalingUpRule.addTarget(
        new targets.LambdaFunction(fn, {
          event: events.RuleTargetInput.fromObject({ message: "Triggering Scheduled Scaling" }),
        })
    );

    // allow the Event Rule to invoke the Lambda function
    targets.addLambdaPermission(ScalingUpRule, fn);

    const ScalingDownRule=new events.Rule(this, 'ScalingDownRule', {
      schedule: events.Schedule.cron({minute:"0", hour:scaleDownHour.valueAsString})
    });


    // add the Lambda function as a target for the Event Rule
    ScalingDownRule.addTarget(
        new targets.LambdaFunction(fn, {
          event: events.RuleTargetInput.fromObject({ message: "Triggering Scheduled Scaling" }),
        })
    );

    // allow the Event Rule to invoke the Lambda function
    targets.addLambdaPermission(ScalingDownRule, fn);
  }
}
