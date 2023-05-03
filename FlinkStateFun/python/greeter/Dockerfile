# From the standard flink-statefun docker image in docker repository
FROM apache/flink-statefun


RUN mkdir -p /opt/statefun/modules/remote
COPY flink-conf.yaml $FLINK_HOME/conf/flink-conf.yaml
COPY module.yaml /opt/statefun/modules/remote/module.yaml


# Is needed if IAM authentication to Amazon MSK is desired
# COPY aws-msk-iam-auth-1.1.0-all.jar $FLINK_HOME/lib/aws-msk-iam-auth-1.1.0-all.jar