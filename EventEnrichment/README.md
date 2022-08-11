
## Architecture
Use the Kinesis Data Generator to generate sample data to Kinesis stream. Flink application running on local Java IDE will consume the data and enrich it with reference data stored in different locations, for example a CSV format in S3.

![High level architecture to enrich events coming from Kinesis Data Streams with the static reference data stored in S3](src/main/resources/arch.jpg)

## Setup

To use the Kinesis Data Generator to generate sample events, we need to setup the Cognito. Follow the instructions (https://awslabs.github.io/amazon-kinesis-data-generator/web/help.html) to create Cognito user pool required for Amazon Kinesis Data Generator along with instruction to setup the events.

Create a Kinesis Data Stream and S3 bucket in a region.

The sample event data in Kinesis Data Generator is generated using the following template -

    {
        "id": "{{random.number(1000)}}",
        "fname":"{{name.firstName}}",
        "lname":"{{name.lastName}}",
        "role":"{{random.arrayElement(["developer","accountant","logistics","front-office","back-office"])}}"
    }


The reference location data (`location_data.csv`) we are using in the S3 for the sample code, looks like this -

    role,location
    developer,Building-I
    accountant,Building-II
    front-office,Building-III
    back-office,Building-IV
    logistics,Building-V

Copy the reference data in the newly created S3 bucket.

## Compile & Execute

Download the code and update the config file with Kinesis Data Stream name, S3 bucket name and region.

Compile it by executing

    mvn clean install

Execute the classes using the following command line

    java -cp target/event-data-enrichment-1.0-SNAPSHOT.jar com.amazonaws.ProcessStreamPreLoadReferenceDataInMemory

    java -cp target/event-data-enrichment-1.0-SNAPSHOT.jar com.amazonaws.ProcessStreamPreLoadReferenceData
    
    java -cp target/event-data-enrichment-1.0-SNAPSHOT.jar com.amazonaws.ProcessStreamPartitionPreLoadReferenceData

    java -cp target/event-data-enrichment-1.0-SNAPSHOT.jar com.amazonaws.ProcessStreamPeriodicPartitionPreLoadReferenceData
