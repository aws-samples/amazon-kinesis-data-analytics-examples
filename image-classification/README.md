# Image Classification using Kinesis Data Analytics on Apache Flink

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

Next, we create a Tumbling window of a variable time window duration, specified in configuration, defaulting to 60 seconds. Every window close creates a batch (list) of images to be classified using a `ProcessWindowFunction`.

This ProcessWindowFunction will call the
classifier predict function on the list of images and return the
best probability of classification from each image. This result is
then sent back to the Flink operator where it is promptly written
out to the Amazon S3 bucket path of your configuration.

```java
.process(new ProcessWindowFunction<StreamedImage, String, String, TimeWindow>() {
                    @Override
                    public void process(String s,
                                        ProcessWindowFunction<StreamedImage, String, String, TimeWindow>.Context context,
                                        Iterable<StreamedImage> iterableImages,
                                        Collector<String> out) throws Exception {


                            List<Image> listOfImages = new ArrayList<Image>();
                            iterableImages.forEach(x -> {
                                listOfImages.add(x.getImage());
                            });
                        try
                        {
                            // batch classify images
                            List<Classifications> list = classifier.predict(listOfImages);
                            for (Classifications classifications : list) {
                                Classifications.Classification cl = classifications.best();
                                String ret = cl.getClassName() + ": " + cl.getProbability();
                                out.collect(ret);
                            }
                        } catch (ModelException | IOException | TranslateException e) {
                            logger.error("Failed predict", e);
                        }
                        }
                    });

```

#### Classfier.java
In Classifier.java, we read the image and apply crop, transpose, reshape, and finally convert to an N-dimensional array that can be processed by the deep learning model. Then we feed the array to the model and apply a forward pass. During the forward pass, the model computes the neural network layer by layer. At last, the output object contains the probabilities for each image object that the model being trained on. We map the probabilities with the object name and return to the map function.

### Dependencies

- Java 11
- Maven
- Apache Flink

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

Note - Be sure to replace the first two line's BUCKET variables with your own source bucket and sink bucket, which will contain the source images and the classifications respectively.

Command 1 – Setting up CDK Environment
If you do not have CDK Bootstrapped in your account, run the following command, substituting your account number and current region:

`cdk bootstrap aws://ACCOUNT-NUMBER/REGION`

Command 2 – Setting up S3 Bucket and Launching AWS CloudFormation Script
The script will clone a GitHub Repo of images to classify and upload them to your source Amazon S3 bucket. Then it will launch the CloudFormation stack given your input parameters.




export SOURCE_BUCKET=s3://SAMPLE-BUCKET/PATH;
export SINK_BUCKET=s3://SAMPLE_BUCKET/PATH;
git clone https://github.com/EliSchwartz/imagenet-sample-images;
cd imagenet-sample-images;
aws s3 cp . $SOURCE_BUCKET --recursive --exclude "*/";
aws cloudformation create-stack --stack-name KDAImageClassification --template-url https://aws-blogs-artifacts-public.s3.amazonaws.com/artifacts/BDB-3098/BlogStack.template.json --parameters ParameterKey=inputBucketPath,ParameterValue=$SOURCE_BUCKET ParameterKey=outputBucketPath,ParameterValue=$SINK_BUCKET --capabilities CAPABILITY_IAM;


The script will clone a Github Repo of images to classify and upload them to your source Amazon S3 bucket. Then it will launch the CloudFormation stack given your input parameters.

This CloudFormation Stack will launch:
- A Kinesis Data Analytics Application with 1 Kinesis Processing Unit preconfigured with some application properties
- An Amazon S3 Bucket for your output results
- IAM Roles for the communication between the two services

Pass our sample S3 bucket as an input into the CloudFormation Template, and provide your own S3 bucket as an output path, including an ending forward-slash.

- `inputBucketPath`: `s3://<<sample-image-bucket>>`
- `outputBucketPath`: `s3://<<personal-bucket>>/`

**provide your own S3 Buckets as parameter**

Once this stack completes launching, navigate to the Kinesis Data Analytics for Apache Flink console and find the application called blog-DJL-flink-ImageClassification-application. Click on Run, then navigate to the Amazon S3 bucket you specified in the `outputBucketPath` variable. If you have readable in the source bucket listed, you should see classifications of those images within the checkpoint interval of the running application. See manual steps below for further steps to validate.

### Dependencies

- Java 11
- Maven
- Apache Flink

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
