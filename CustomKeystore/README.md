# Custom keystore for Kafka connector

> #### ⚠️ This example and the pattern described are obsolete.
> Please refer to the [example solving the same problem](https://github.com/aws-samples/amazon-managed-service-for-apache-flink-examples/tree/main/java/KafkaCustomKeystoreWithConfigProviders), 
> in the new [Amazon Managed Service for Apache Flink examples](https://github.com/aws-samples/amazon-managed-service-for-apache-flink-examples) repository

This sample shows how to include certificate files associated with [custom keystore and truststore for Kafka](https://kafka.apache.org/documentation/streams/developer-guide/security.html#id2). When using the Flink connector for Kafka, you 
have to specify the truststore location. In order to ensure that the certificate files are available on each of the nodes on which the connector runs, we have to perform a custom initialization.

For further details on how to configure the cluster and the Kinesis Data Analytics application, refer to the instructions in [custom truststore with Amazon MSK](https://docs.aws.amazon.com/kinesisanalytics/latest/java/example-keystore.html).

## Steps for building/packaging

1. Include your cert(s) under the `src/main/resources/` folder.
2. The code that grabs the cert(s) from the resources folder is in [CustomFlinkKafkaUtil.java](src/main/java/com/amazonaws/services/kinesisanalytics/CustomFlinkKafkaUtil.java). You can change the name(s) of the cert(s) to reflect what's in this 
class or change the code to reflect the name(s) of your cert(s).
3. Build the project:

```
> mvn package -Dflink.version=1.15.2
```

After running the above command, you should see the built jar file under the `target/` folder:

```
> ls -alh target/
...
drwxr-xr-x   8 user  group   256B Aug 26 13:36 .
drwxr-xr-x  11 user  group   352B Aug 27 08:29 ..
-rw-r--r--   1 user  group   3.6M Aug 26 13:36 flink-app-1.0-SNAPSHOT.jar

```

## Implementation details

For those interested, here's a deeper look at the implementation details:

1. Package the certificate files as a resource in containing jar. NOTE: Please include your own certs in the src/main/resources folder.
2. Extend the SimpleStringSerializationSchema and SimpleStringDeserializationSchema classes so that we can perform initialization of the truststore on the open method.
3. Drop our certs in /tmp during connector initialization so it'll be available when the connector runs.
4. Please pay close attention to the included [POM file](pom.xml) for details on which dependent libraries are necessary and how to configure them.
