# Packaging Instructrions for PyFlink on Kinesis Data Analytics

## Copy contents of site-packges

Copy the files and folders that are located within your python environments `site-packages` folder in this folder. 
If you are using boto3 with Amazon Kinesis Data Analytics your file structure will look similar to this:

```
PythonPackages
│   README.md
│   python-packages.py    
│
└───boto3
│   │   session.py
│   │   utils.py
│   │   ...
│   
└───botocore
│   │   args.py
│   │   auth.py
│       ...
└───lib
│    │ flink-sql-connector-kinesis-1.15.2.jar 
│    │ ...
...

```

## Ensure your used connector is included

Copy the version of the connector you are using (e.g. `flink-sql-connector-kinesis-1.15.2.jar`) in the lib directory. 
Make sure the connector version corresponds to the used Apache Flink version in Kinesis Data Analytics.

## Package the application

For packaging your application **DON'T** use the compress tool in Finder or Windows Explorer. 
This will create an invalid code package for KDA. On Linux / macOS you can use the following command:
```shell
zip kdaApp.zip * lib/*
```

## Set application properties in KDA

Add the application properties to the KDA application as defined in `application_properties.json`. 
There is no need to define a `pyFiles` property,
as KDA will unzip the application code and the additional packages will be available in the base path.