from aws_cdk import (
    # Duration,
    Stack,
    aws_kinesisanalytics_flink_alpha as flink,
    aws_s3 as s3,
    aws_iam as iam
)
from constructs import Construct
import os

class BlogStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        s3_sink_bucket = s3.Bucket(
            self,
            'flink_blog_code',
            bucket_name = f"flink-blog-bucket-{os.getenv('CDK_DEFAULT_ACCOUNT')}-{os.getenv('CDK_DEFAULT_REGION')}"
        )
        
        app_bucket = s3.Bucket.from_bucket_name(self, "code_bucket", "randombucketgaurav")
        file_key = "embedded-model-inference-1.0-SNAPSHOT.jar"
        
        flink_app = flink.Application(self, "flink_blog_app",
            code=flink.ApplicationCode.from_bucket(app_bucket, file_key),
            property_groups={
                "appProperties": {
                    "s3.source.path": f"s3://{s3_sink_bucket.bucket_name}/input/",
                    "s3.sink.path": f"s3://{s3_sink_bucket.bucket_name}/output/"
                }
            },
            #code=flink.ApplicationCode.from_asset("./jar/embedded-model-inference-1.0-SNAPSHOT.jar"),
            runtime=flink.Runtime.FLINK_1_15
        )
        
        flink_app.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[f"arn:aws:s3:::{s3_sink_bucket.bucket_name}/*",
                           f"arn:aws:s3:::{s3_sink_bucket.bucket_name}"],
                actions=[
                    's3:GetObject',
                    's3:ListBucket',
                    's3:PutObject'
                ]
            )
        )
