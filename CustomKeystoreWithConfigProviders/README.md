# Sample illustrating how to use MSK config providers in Flink Kafka connectors

This sample illustrates how to configure the Flink Kafka connectors (KafkaSource and KafkaSink) with keystore and/or truststore certs using the MSK config providers described [here](https://github.com/aws-samples/msk-config-providers).

## High level approach

NOTE: Steps 1 and 2 are optional because this repo already includes a previously built version of the msk-config-providers jar

1. (Optional) Clone and build [MSK config providers repo](https://github.com/aws-samples/msk-config-providers).
2. (Optional) Pull in the built jar into `local-repo` in this repo (See [pom.xml](pom.xml)).
3. Build this repo using `mvn clean package`.
4. Setup Flink app using the jar from the build above. Please follow the instructions [here](https://docs.aws.amazon.com/kinesisanalytics/latest/java/getting-started.html).
5. Please ensure that you specify appropriate values for the application properties (S3 location, secrets manager key, etc...).

## Configuring the Kafka connector

The following snippet shows how to configure the service providers and other relevant properties.

See [StreamingJob.java](src/main/java/com/amazonaws/services/kinesisanalytics/StreamingJob.java):

```java
...
// define names of config providers:
builder.setProperty("config.providers", "secretsmanager,s3import");

// provide implementation classes for each provider:
builder.setProperty("config.providers.secretsmanager.class", "com.amazonaws.kafka.config.providers.SecretsManagerConfigProvider");
builder.setProperty("config.providers.s3import.class", "com.amazonaws.kafka.config.providers.S3ImportConfigProvider");

String region = appProperties.get(Helpers.S3_BUCKET_REGION_KEY).toString();
String keystoreS3Bucket = appProperties.get(Helpers.KEYSTORE_S3_BUCKET_KEY).toString();
String keystoreS3Path = appProperties.get(Helpers.KEYSTORE_S3_PATH_KEY).toString();
String truststoreS3Bucket = appProperties.get(Helpers.TRUSTSTORE_S3_BUCKET_KEY).toString();
String truststoreS3Path = appProperties.get(Helpers.TRUSTSTORE_S3_PATH_KEY).toString();
String keystorePassSecret = appProperties.get(Helpers.KEYSTORE_PASS_SECRET_KEY).toString();
String keystorePassSecretField = appProperties.get(Helpers.KEYSTORE_PASS_SECRET_FIELD_KEY).toString();

// region, etc..
builder.setProperty("config.providers.s3import.param.region", region);

// properties
builder.setProperty("ssl.truststore.location", "${s3import:" + region + ":" + truststoreS3Bucket + "/" + truststoreS3Path + "}");
builder.setProperty("ssl.keystore.type", "PKCS12");
builder.setProperty("ssl.keystore.location", "${s3import:" + region + ":" + keystoreS3Bucket + "/" + keystoreS3Path + "}");
builder.setProperty("ssl.keystore.password", "${secretsmanager:" + keystorePassSecret + ":" + keystorePassSecretField + "}");
builder.setProperty("ssl.key.password", "${secretsmanager:" + keystorePassSecret + ":" + keystorePassSecretField + "}");
...
```
