aws emr create-cluster --release-label emr-6.4.0 \
--applications Name=Flink \
--name FlinkHudiCluster \
--configurations file://./configurations.json \
--region us-east-1 \
--log-uri s3://aws-logs-216332718170-us-east-1/elasticmapreduce/ \
--instance-type m5.xlarge \
--instance-count 2 \
--service-role EMR_DefaultRole \
--ec2-attributes KeyName=sub-account-key-east-1,InstanceProfile=EMR_EC2_DefaultRole,SubnetId=subnet-07c86807046476883