# Image Recognition using Kinesis Data Analytics on Apahce Flink

In this code sample, we leverage the [Deep Java Library](https://djl.ai/), an open source, high-level, engine agnostic Java framework for deep learning, in order to classify images using Apache Flink on Kinesis Data Analytics. 

## Description

![Architecture](img/Picture1.png)

This codebase leverages the Apache Flink [Filesystem Connector File Source](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/filesystem/#file-source) to read files from Amazon S3. 

You can set the bucket this application reads by changing the variable called `bucket` in `EMI.java`, or setting the application property on Kinesis Data Analytics for Apache Flink once deployed.

```java
final FileSource<StreamedImage> source = 
        FileSource.forRecordStreamFormat(new ImageReaderFormat(), 
        new Path(s3SourcePath))
        .monitorContinuously(Duration.ofSeconds(10))
        .build();
```

The File Source is configured to read in files in the ImageReaderFormat, and will check Amazon S3 for new images every 10 seconds. This can be configured as well.

Once we have read in our images, we then convert our FileSource into a stream that can be processed.

```java
DataStream<StreamedImage> stream = 
        env.fromSource(source, WatermarkStrategy.noWatermarks()
        "file-source");
```

You can modify the listOfImagesBufferSize to make larger or smaller
buffered images. This can be configured on Kinesis Data Analytics
for Apache Flink by using the application property image.buffer
size, but is defaulted to 100 images at a time.

```java
DataStream<List<StreamedImage>> listOfImagesStream = 
        stream.keyBy(x-> x.getId())
        .process(new CollectImagesInList(listOfImagesBufferSize));
```

Finally, we have our batches in the stream and are ready to send them to a flatMap function for classifying images via the
Classifier.java class. This flatMap function will call the
classifier predict function on the list of images and return the
best probability of classification from each image. This result is
then sent back to the Flink operator where it is promptly written
out to the Amazon S3 bucket path of your configuration.

#### Classfier.java
In Classifier.java, we read the image and apply crop, transpose, reshape, and finally convert to an N-dimensional array that can be processed by the deep learning model. Then we feed the array to the model and apply a forward pass. During the forward pass, the model computes the neural network layer by layer. At last, the output object contains the probabilities for each image object that the model being trained on. We map the probabilities with the object name and return to the map function.

### Dependencies

- Java 11
- Maven
- Apache Flink
- 
## Getting Started
After cloning the application locally, you can create your application jar by navigating to the directory that contains your pom.xml and running the following command:

```bash
mvn clean compile package
```

This will build your application jar in the `target/` directory called `embedded-model-inference-1.0-SNAPSHOT.jar.` Upload this application jar to an Amazon S3 bucket, either the one created from the CloudFormation template above, or another one to store code artifacts.

You can then configure your Kinesis Data Analytics application to point to this newly uploaded Amazon S3 jar file as described in the [Getting Started guide for Kinesis Data Analytics for Apache Flink](https://docs.aws.amazon.com/kinesisanalytics/latest/java/get-started-exercise.html#get-started-exercise-6). This is also a great opportunity to configure your runtime properties. (See example below)

![Runtime Properties](img/runtime-properties.png)

Once done configuring, hit Run to start your Apache Flink application. You can open the Apache Flink Dashboard to check for application exceptions or to see data flowing through the tasks defined in code.

![Flink Dashboard](img/flink-dashboard.png)

To validate our results, let’s check the results in Amazon S3 by navigating over to the Amazon S3 Console and finding our Amazon S3 Bucket.

We can find the output in a folder called output-kda.
![Output](img/output.png)

When we click on one of the data-partitioned folders, we will see partition files. Ensure that there is no underscore in front of your part file, as this will indicate the results are still being finalized according to the rollover interval defined in Apache Flink’s FileSink connector. Once the underscores have disappeared, we can use Amazon S3 Select to view our data:

![Partition Files](img/partition-files.png)

### CloudFormation Script

In order to run this codebase on Kinesis Data Analytics for Apache Flink, we have a helpful CloudFormation template that will spin up the necessary resources:

[![Launch Stack](img/launch-stack.png)](https://us-east-1.console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/create/review?templateURL=https://aws-blogs-artifacts-public.s3.amazonaws.com/artifacts/BDB-3098/BlogStack.template.json&stackName=ImageRecognitionKDA&param_inputBucketPath=s3://aws-blogs-artifacts-public/artifacts/BDB-3098/images/)

This CloudFormation Stack will launch:
- A Kinesis Data Analytics Application with 1 Kinesis Processing Unit preconfigured with some application properties
- An Amazon S3 Bucket for your output results
- IAM Roles for the communication between the two services

Pass our sample S3 bucket as an input into the CloudFormation Template, and provide your own S3 bucket as an output path, including an ending forward-slash.

- inputBucketPath: <<Sample-Image-Bucket>>
- outputBucketPath: s3://<<personal-bucket>>/ 
**provide your own S3 Bucket as parameter**

Once this stack completes launching, navigate to the Kinesis Data Analytics for Apache Flink console and find the application called blog-DJL-flink-ImageRecognition-application. Click on Run, then navigate to the Amazon S3 bucket you specified in the `outputBucketPath` variable. If you have readable in the source bucket listed, you should see classifications of those images within the checkpoint interval of the running application. See manual steps below for further steps to validate.

### Dependencies

- Java 11
- Maven
- Apache Flink
-
## Getting Started
After cloning the application locally, you can create your application jar by navigating to the directory that contains your pom.xml and running the following command:

```bash
mvn clean compile package
```

This will build your application jar in the `target/` directory called `embedded-model-inference-1.0-SNAPSHOT.jar.` Upload this application jar to an Amazon S3 bucket, either the one created from the CloudFormation template above, or another one to store code artifacts.

You can then configure your Kinesis Data Analytics application to point to this newly uploaded Amazon S3 jar file as described in the [Getting Started guide for Kinesis Data Analytics for Apache Flink](https://docs.aws.amazon.com/kinesisanalytics/latest/java/get-started-exercise.html#get-started-exercise-6). This is also a great opportunity to configure your runtime properties. (See example below)

![Runtime Properties](img/runtime-properties.png)

Once done configuring, hit Run to start your Apache Flink application. You can open the Apache Flink Dashboard to check for application exceptions or to see data flowing through the tasks defined in code.

![Flink Dashboard](img/flink-dashboard.png)

To validate our results, let’s check the results in Amazon S3 by navigating over to the Amazon S3 Console and finding our Amazon S3 Bucket.

We can find the output in a folder called output-kda.
![Output](img/output.png)

When we click on one of the data-partitioned folders, we will see partition files. Ensure that there is no underscore in front of your part file, as this will indicate the results are still being finalized according to the rollover interval defined in Apache Flink’s FileSink connector. Once the underscores have disappeared, we can use Amazon S3 Select to view our data:

![Partition Files](img/partition-files.png)

## Authors

- Jeremy Ber
- Can Taylan Sari
- Gaurav Rele