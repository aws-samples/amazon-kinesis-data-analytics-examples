import sbt.Keys.javacOptions

lazy val root = (project in file(".")).
  settings(
    name := "getting-started-scala",
    version := "1.0",
    scalaVersion := "3.2.0",
    mainClass := Some("com.amazonaws.services.kinesisanalytics.main"),
    javacOptions ++= Seq("-source", "11", "-target", "11")
  )

val jarName = "getting-started-scala-1.0.jar"
val flinkVersion = "1.15.2"
val kdaRuntimeVersion = "1.2.0"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-kinesisanalytics-runtime" % kdaRuntimeVersion,
  "org.apache.flink" % "flink-connector-kinesis" % flinkVersion,
  "org.apache.flink" % "flink-streaming-java" % flinkVersion % "provided"
)

artifactName := { (_: ScalaVersion, _: ModuleID, _: Artifact) => jarName }

assembly / assemblyJarName := jarName
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.discard
    }
  case _ => MergeStrategy.first
}