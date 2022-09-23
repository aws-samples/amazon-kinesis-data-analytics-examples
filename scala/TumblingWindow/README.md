# KDA Scala Tumbling Window Example
This a simple streaming application which uses Scala `3.2.0` and Flink's Java DataStream API.
The application reads data from Kinesis stream, aggregates it using tumbling windows and writes results to output Kinesis stream.

### Build
- Run `sbt assembly` to build an uber jar 
- Use `target/scala-3.2.0/tumbling-window-scala-1.0.jar` in your KDA application