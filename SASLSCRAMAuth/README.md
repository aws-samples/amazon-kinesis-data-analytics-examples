## SASL/SCRAM auth for MSK <=> KDA

Additional background can be found here: [Amazon Managed Streaming for Kafka - Username and password authentication with AWS Secrets Manager](https://docs.aws.amazon.com/msk/latest/developerguide/msk-password.html)

SASL/SCRAM requires us to specify a login module via jaas config as shown below:

```
sasl.jaas.config = org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="pass";
```

In order to be able to reference the ScramLoginModule in Kinesis Data Analytics apps we need to build a fat jar containing the Kafka SQL/Table API connector as well as the kafka-clients library. This sample illustrates how to do that.

NOTE: The kafka-clients library needs to be included *without relocation* so that the SASL/SCRAM login module that we've specified in the jaas config above can be found. This sample uses *Flink 1.13* but you could follow a similar approach for other versions of Flink as well.

### Building and generating fat jar

This repo does not contain any code. All the magic is in the POM file - specifically, the addition of the `kafka-clients` entry. You simply have to generate the jar using the following command:

```
> mvn package
```

After running the above command, you should see the built jar file under the `target/` folder:

```
> ls -alh target/
...
drwxr-xr-x   8 user  group   256B Aug 26 13:36 .
drwxr-xr-x  11 user  group   352B Aug 27 08:29 ..
-rw-r--r--   1 user  group   3.6M Aug 26 13:36 SASLScramLoginFatJar-0.1.jar

```

## Including this custom connector in a Kinesis Data Analytics Python Table API app

You can include the above jar file as a dependency of your Python Table API app using the `jarfile` property under the `kinesis.analytics.flink.run.options` property group. Please see this [walkthrough](https://docs.aws.amazon.com/kinesisanalytics/latest/java/gs-python-createapp.html) for more details.

## Including this custom connector in a Kinesis Data Analytics Studio app

This [article](https://docs.aws.amazon.com/kinesisanalytics/latest/java/how-zeppelin-connectors.html) describes how connectors and dependencies work in Kinesis Data Analytics Studio. As described in that article, each Kinesis Data Analytics Studio notebook comes with default connectors, of which the Kafka SQL connector is one. To use the fat jar from this repo:

1. Remove the default Kafka SQL connector
2. Add the SASLScramLoginFatJar-*.jar

## Configuring Kafka connector in DDL for SASL/SCRAM

Here's a sample DDL statement:

```
CREATE TABLE IF NOT EXISTS kafka_sink (
    entry STRING
)
WITH (
    'connector'= 'kafka',
    'format' = 'csv',
    'topic' = 'MyTopic',
    'properties.bootstrap.servers' = '<kafka-bootstrap-servers>',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'SCRAM-SHA-512',
    'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.scram.ScramLoginModule required username="<your username>" password="<your password>";'
)
```

Please make sure that you replace the bootstrap servers along with username and password in the jaas.config entry above.