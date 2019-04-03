package pack

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.{ClientConfiguration, Protocol, SDKGlobalConfiguration}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Example {

  val accessKey = "***"
  val secretKey = "***"
  val bucketName = "***"

  /**
   * Creates a spark session with all the required settings.
   *
   * @return spark session.
   */
  def createSpark(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("App")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
    System.setProperty(SDKGlobalConfiguration.ENABLE_S3_SIGV4_SYSTEM_PROPERTY, "true")

    spark
  }

  /**
   * Creates amazon s3 client with all the required settings.
   *
   * @return an s3 client instance.
   */
  def createAmazonS3Client(): AmazonS3Client = {
    val credentials = new BasicAWSCredentials(accessKey, secretKey)
    val s3client = new AmazonS3Client(credentials, new ClientConfiguration().withProtocol(Protocol.HTTP))
    s3client.setEndpoint("s3.us-east-2.amazonaws.com")

    s3client
  }

  def main(args: Array[String]): Unit = {

    val spark = createSpark()
    val s3Client = createAmazonS3Client()


    if (!S3DirectoryManipulator.doesPathExist(s3Client, bucketName, "output")) {
      S3DirectoryManipulator.createFolder(s3Client, bucketName, "output/")
    }

    //renamePathsWithPrefix(s3client, bucketName, "testFolder", "tmp")

    val df = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("s3a://pavel.orekhov.testing.bucket/taxonomy.csv")
      .withColumn("date_load", lit(getCurrentDate))

    df.repartition(1)
      .write
      .partitionBy("date_load")
      .mode("overwrite")
      .option("header", "true")
      .csv("s3a://pavel.orekhov.testing.bucket/output/")

    println(df.count())
  }


  def getCurrentDate: String = {
    val dtf = DateTimeFormatter.ofPattern("yyyyMMdd")
    LocalDateTime.now.format(dtf)
  }
}
