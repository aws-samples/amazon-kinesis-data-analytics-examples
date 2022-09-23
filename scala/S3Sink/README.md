# KDA Scala Tumbling Window Example
This a simple streaming application which uses Scala `3.2.0` and Flink's Java DataStream API.
The application reads data from Kinesis stream, aggregates it using sliding windows and writes results to S3.

### Build
- Run `sbt assembly` to build an uber jar 
- Use `target/scala-3.2.0/s3-sink-scala-1.0.jar` in your KDA application