# Sample illustrating how to use delta-flink connector on Kinesis Data Analytics to load Delta tables

This example demonstrates how to sink data to Delta tables and source data from Delta tables using Kinesis Data Analytics with Flink. The example uses delta-flink connector which provides Java DataStream API to interact with Delta tables using flink.
There are two applications in this example.
1) Application 1 (DeltaSink directory) demonstrates sinking data from Kinesis data streams to Delta table using Kinesis Data Analytics with flink. The data is being appended to delta table, no upserts or overwrites is done. This is due to current limitations of delta-flink connector.
2) Application 2 (DeltaSource directory) demonstrates sourcing data from existing Delta table in a streaming fashion, aggregating data (max price per ticker) using event time processing with tumbling window and sinking it to another Delta table.  (Note: for the sake of this example sink location has been choosen as Delta table but it could be a different database such as, time series, opensearch etc)

## Architecture

Application 1 Architecture Diagram (Delta Sink)

![Example 1 delta sink architecture diagram](img/example1_arch.png)

Application 2 Architecture Diagram (Delta Source)
![Example 2 delta source architecture diagram](img/example2_arch.png)


## Setup

Create a Kinesis data stream and start streaming random stock ticker data. Follow the instructions (https://docs.aws.amazon.com/code-library/latest/ug/kinesis-analytics-v2_example_kinesis-analytics-v2_DataGenerator_StockTicker_section.html)
 

Create an S3 bucket in the same region where Kinesis Data Analytics applications are running.
The bucket will be used to sink/source delta tables.


## Build

Download the code and update .java files with Kinesis Data Stream name, S3 bucket name and region.

Change to corresponding application directory and build it by executing following:

    mvn package -Dflink.version=1.15.4

After running the above command, you should see the built jar file under the target/ folder.
 


## Execute 
Place jar files into the chosen S3 bucket and configure Kinesis Data Analytics application code location to point to .jar file.

Start streaming data into Kinesis Data Stream and run first KDA application. After some minutes you should see parquet files and
_delta_log folder were created /tickers folder in the S3 bucket.

Let first application run. Create another KDA application and configure its application code to point to .jar file that was 
built from /DeltaSource folder. Run second KDA application, after some minutes you should see parquet files and
_delta_log folder were created under /tickers_agg folder in the S3 bucket.



## Resources

https://github.com/delta-io/connectors/blob/master/flink/README.md