name := "kafka-reader"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.0"

val sparkDependencies = Seq(
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % sparkVersion,
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "org.apache.spark" % "spark-hive_2.11" % sparkVersion % "provided",
  "org.apache.spark" % "spark-streaming_2.11" % sparkVersion,
  "org.apache.kafka" %% "kafka" % "2.2.0"

)

libraryDependencies ++= sparkDependencies