name := "kafka-reader"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.2.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"