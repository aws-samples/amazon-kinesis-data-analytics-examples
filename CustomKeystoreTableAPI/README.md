# Custom keystore for Table API Kafka connector

This sample shows how to drop certificate files associated with [custom keystore and truststore for Kafka](https://kafka.apache.org/documentation/streams/developer-guide/security.html#id2). When using the Flink Table API connector for Kafka, you have to specify the truststore location (when using TLS for encryption in transit) and the keystore location (when using mTLS for authentication). In order to ensure that the certificate files are available on each of the nodes on which the connector runs, we have to do custom initialization. Note that this initialization approach is different for the Datastream connector - [see this sample](https://docs.aws.amazon.com/kinesisanalytics/latest/java/example-keystore.html).

This sample also shows how to package a fat jar containing both the underlying Kafka connector as well as our custom initialization.

IMPORTANT: This sample targets the Kafka connector in Flink 1.13 specifically. For other versions, you'd have to follow a similar approach overall but the details may differ.

## Steps for building/packaging

1. Include your cert(s) under the `src/main/resources/` folder. Note that you need to supply the certs; if you you're using mTLS auth with MSK, please see this [doc](https://docs.aws.amazon.com/msk/latest/developerguide/msk-authentication.html) for detail on how to generate certs.
2. The code that grabs the cert(s) from the resources folder is in [CustomFlinkKafkaUtil.java](src/main/java/com/amazonaws/services/kinesisanalytics/CustomFlinkKafkaUtil.java). You can change the name(s) of the cert(s) to reflect what's in this class or change the code to reflect the name(s) of your cert(s). If you want the cert(s) dropped in a folder other than `/tmp`, you can update this code (If you do, make sure you update the respective connector DDL). And lastly, if you have more than one cert (for instance, keystore *and* truststore), you can update the code here.
3. Create fat jar by running the following command from the root folder of this project:

```
> mvn package
```

After running the above command, you should see the built jar file under the `target/` folder:

```
> ls -alh target/
...
drwxr-xr-x   8 user  group   256B Aug 26 13:36 .
drwxr-xr-x  11 user  group   352B Aug 27 08:29 ..
-rw-r--r--   1 user  group   3.6M Aug 26 13:36 TableAPICustomKeystore-0.1.jar

```

## Including this custom connector in a Kinesis Data Analytics Python Table API app

You can include the above jar file as a dependency of your Python Table API app using the `jarfile` property under the `kinesis.analytics.flink.run.options` property group. Please see this [walkthrough](https://docs.aws.amazon.com/kinesisanalytics/latest/java/gs-python-createapp.html) for more details.

## Including this custom connector in a Kinesis Data Analytics Studio app

This [article](https://docs.aws.amazon.com/kinesisanalytics/latest/java/how-zeppelin-connectors.html) describes how connectors and dependencies work in Kinesis Data Analytics Studio. As described in that article, each Kinesis Data Analytics Studio notebook comes with default connectors, of which the Kafka SQL connector is one. To use the custom Table API connector from this repo:

1. Remove the default Kafka SQL connector
2. Add the custom Table API connector

## Referencing the custom connector in DDL

Here's a sample DDL statement:

```
CREATE TABLE IF NOT EXISTS kafka_sink (
   entry STRING
)
WITH (
   'connector'= 'customkafka',
   'format' = 'raw',
   'topic' = 'MyKafkaTopic',
   'properties.bootstrap.servers' = '<kafka-bootstrap-servers>',
   'properties.security.protocol' = 'SSL',
   'properties.ssl.keystore.type' = 'PKCS12',
   'properties.ssl.keystore.location' = '/tmp/kafka.client.keystore.jks',
   'properties.ssl.keystore.password' = '<your keystore password>',
   'properties.ssl.key.password' = '<your key password>'
)
```

Note that the connector name above is `'customkafka'` instead of `'kafka'`. This is specified in [`CustomKafkaDynamicTableFactory.java`](src/main/java/com/amazonaws/services/kinesisanalytics/overrides/CustomKafkaDynamicTableFactory.java:). Also, please make sure that you replace the bootstrap servers and key/keystore passwords above.

## Implementation details

For those interested, here's a deeper look at the implementation details:

1. Package the certificate files as a resource in containing jar. NOTE: Please include your own certs in the src/main/resources folder.
2. Extend the Kafka DynamicTableFactory class so we can perform initialization. Background on DynamicTableFactory can be found [here](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sourcessinks/#dynamic-table-factories).
   - Overriden methods can be found in [CustomKafkaDynamicTableFactory.java](src/main/java/com/amazonaws/services/kinesisanalytics/overrides/CustomKafkaDynamicTableFactory.java).
   - As mentioned in the [Flink docs](https://nightlies.apache.org/flink/flink-docs-release-1.15/api/java/org/apache/flink/table/factories/TableFactory.html), we have to update [META-INF/services/org.apache.flink.table.factories.Factory](src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory) to include our custom class that overrides DynamicTableFactory. This allows Flink to discover our custom connector override.
3. Drop our certs in /tmp during connector initialization so it'll be available when the connector runs.
4. Please pay close attention to the included [POM file](pom.xml) for details on which dependent libraries are necessary and how to configure them.
5. Turn off resource filtering for certs (https://stackoverflow.com/questions/23126282/java-apns-certificate-error-with-derinputstream-getlength-lengthtag-109-too). Also: https://stackoverflow.com/questions/34749819/maven-resource-filtering-exclude
6. Ensure that you specify cert type (PKCS12) in table connector.
7. Turn off resource filtering for jks extensions in the [POM file](pom.xml) using the maven-resources-plugin plugin to ensure that cert files don't get modified.