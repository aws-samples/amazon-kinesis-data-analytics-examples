# KDA Scala Example
This a simple streaming application which uses Scala `3.2.0` and Flink's Java DataStream API for reading data from and writing to Kinesis streams.

### Pre-requisites
You need to have `sbt` tool installed on you machine to build a Scala project. Use [steps from official guide](https://www.scala-sbt.org/download.html) to do that.

### Build
- Run `sbt assembly` to build an uber jar 
- Use `target/scala-3.2.0/getting-started-scala-1.0.jar` in your KDA application