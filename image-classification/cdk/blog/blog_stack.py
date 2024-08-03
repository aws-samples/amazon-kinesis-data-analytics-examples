"""
/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
"""
from aws_cdk import (
    # Duration,
    Stack,
    CfnParameter,
    Fn,
    aws_kinesisanalytics_flink_alpha as flink,
    aws_s3 as s3,
    aws_iam as iam
)
from constructs import Construct
import os

class BlogStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        input_bucket_path = CfnParameter(self, "inputBucketPath",
                                         type="String",
                                         description="Add the path to the input files",
                                         default='').value_as_string
        output_bucket_path = CfnParameter(self, "outputBucketPath",
                                          type="String",
                                          description="Add the path to the output files",
                                          default='').value_as_string

        # Get bucket names from params for permissions
        input_bucket = Fn.select(2, Fn.split("/", input_bucket_path))
        output_bucket = Fn.select(2, Fn.split("/", output_bucket_path))

        # s3_sink_bucket = s3.Bucket(
        #     self,
        #     'flink_blog_code',
        #     bucket_name = f"flink-blog-bucket-{os.getenv('CDK_DEFAULT_ACCOUNT')}-{os.getenv('CDK_DEFAULT_REGION')}"
        # )

        app_bucket = s3.Bucket.from_bucket_name(self, "code_bucket", "aws-blogs-artifacts-public")
        file_key = "artifacts/BDB-3098/embedded-model-inference-1.0-SNAPSHOT.jar"

        flink_app = flink.Application(self, "flink_blog_app",
            application_name="blog-DJL-flink-ImageClassification-application",
            code=flink.ApplicationCode.from_bucket(app_bucket, file_key),
            property_groups={
                "appProperties": {
                    "s3.source.path": input_bucket_path,
                    "s3.sink.path": output_bucket_path
                }
            },
            #code=flink.ApplicationCode.from_asset("./jar/embedded-model-inference-1.0-SNAPSHOT.jar"),
            runtime=flink.Runtime.FLINK_1_15
        )

        flink_app.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[f"arn:aws:s3:::{input_bucket}/*",
                           f"arn:aws:s3:::{input_bucket}",
                           f"arn:aws:s3:::{output_bucket}/*",
                           f"arn:aws:s3:::{output_bucket}"],
                actions=[
                    's3:GetObject',
                    's3:ListBucket',
                    's3:PutObject',
                    's3:DeleteObject'

                ]
            )
        )
