# Packaging Instructions for multiple connectors in Kinesis Data Analytics

If you need to use multiple connectors in your streaming application, you will need to create a fat jar, bundle it with your application and reference it in your application configuration as described [here](https://docs.aws.amazon.com/kinesisanalytics/latest/java/gs-python-createapp.html). This sample application shows how to bundle multiple connectors into a fat jar. 

Pre-requisites:
1. Apache Maven
2. A Kinesis Data Stream and Kinesis Data Firehose Stream
3. (If running locally) Apache Flink installed and appropriate AWS credentials to access Kinesis and Firehose streams

To get this sample application working locally:
1. Run `mvn clean package` in the FirehoseSink folder
2. Ensure the resulting jar is referenced correctly in the python script
3. Ensure the `application_properties.json` parameters are configured correctly
4. Set the environment variable `IS_LOCAL=True` 
5. Run the python script `python streaming-firehose-sink.py`

To get this sample application working in Kinesis Data Analytics:
1. Run `mvn clean package` in the FirehoseSink folder
2. Zip the python script and fat jar generated in the previous step
3. Upload the zip to an in region S3 bucket
4. Create the Kinesis Data Analytics application
5. Configure the application to use the zip uploaded to the S3 bucket and configure the application IAM role to be able to access both Kinesis and Firehose streams
6. Run the application

A sample script to produce appropriate Kinesis records, as well as detailed configuration instructions for Kinesis Data Analytics can be found [here](https://docs.aws.amazon.com/kinesisanalytics/latest/java/gs-python-createapp.html).