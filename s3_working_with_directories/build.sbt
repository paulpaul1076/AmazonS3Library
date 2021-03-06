name := "s3_reading"

version := "0.1"

scalaVersion := "2.11.8"

dependencyOverrides += "com.amazonaws" % "aws-java-sdk" % "1.7.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.2"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.2"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.1"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.7.4"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.294"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1"
libraryDependencies += "org.scala-lang.modules" %% "scala-java8-compat" % "0.8.0" // for ??? impl missing

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.12" % Test,
  "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
  "org.mockito" % "mockito-core" % "2.7.22" % Test
)