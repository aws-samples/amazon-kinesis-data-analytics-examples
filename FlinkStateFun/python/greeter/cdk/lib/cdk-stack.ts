import * as cdk from '@aws-cdk/core';
import * as apigateway from "@aws-cdk/aws-apigateway";
import * as lambda from "@aws-cdk/aws-lambda";
import * as kinesis from "@aws-cdk/aws-kinesis";
import { PythonFunction } from "@aws-cdk/aws-lambda-python";
import { Duration } from '@aws-cdk/core';

export class CdkStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const api = new apigateway.RestApi(this, "statefun-kda", {
      binaryMediaTypes: ['*/*']
    });


    const personLambda =  new PythonFunction(this, 'person', {
      runtime: lambda.Runtime.PYTHON_3_9,
      entry: 'lambda',
      timeout: Duration.seconds(5)
    });

    const personResource = api.root.addResource('person');

    const personIntegration = new apigateway.LambdaIntegration(personLambda, {
      contentHandling: apigateway.ContentHandling.CONVERT_TO_BINARY
    });

    personResource.addMethod('POST', personIntegration);


    const greeterLambda =  new PythonFunction(this, 'greeter', {
      runtime: lambda.Runtime.PYTHON_3_9,
      entry: 'lambda',
      timeout: Duration.seconds(5),
    });

    const greeterResource = api.root.addResource('greeter');

    const greeterIntegration = new apigateway.LambdaIntegration(greeterLambda, {
      contentHandling: apigateway.ContentHandling.CONVERT_TO_BINARY
    });

    greeterResource.addMethod('POST', greeterIntegration);

  }
}