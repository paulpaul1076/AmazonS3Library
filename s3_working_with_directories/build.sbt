name := "s3_reading"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.2"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.2"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.1"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.7.4"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1"
