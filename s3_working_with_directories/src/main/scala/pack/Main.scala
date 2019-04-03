package pack

import java.io.ByteArrayInputStream
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.regex.Pattern

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
import com.amazonaws.{ClientConfiguration, Protocol, SDKGlobalConfiguration}
import org.apache.spark.sql.SparkSession
import com.amazonaws.services.s3.model.ListObjectsRequest

import org.apache.spark.sql.functions.lit

import scala.util.control.Breaks

object Main {

  val accessKey = "***"
  val secretKey = "***"
  val bucketName = "***"

  /**
   * Creates a spark session with all the required settings.
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

    // TODO: uncomment if you want to test

    //    if (!doesPathExist(s3client, bucketName, "output")) {
    //      createFolder(s3client, bucketName, "output/")
    //    }
    //
    //    //renamePathsWithPrefix(s3client, bucketName, "testFolder", "tmp")
    //
    //    val df = spark.read
    //      .option("inferSchema", "true")
    //      .option("header", "true")
    //      .csv("s3a://pavel.orekhov.testing.bucket/taxonomy.csv")
    //      .withColumn("date_load", lit(getCurrentDate))
    //
    //    df.repartition(1)
    //      .write
    //      .partitionBy("date_load")
    //      .mode("overwrite")
    //      .option("header", "true")
    //      .csv("s3a://pavel.orekhov.testing.bucket/output/")
    //
    //    println(df.count())
  }

  // TODO: uncomment if you want to test

  //  def getCurrentDate: String = {
  //    val dtf = DateTimeFormatter.ofPattern("yyyyMMdd")
  //    LocalDateTime.now.format(dtf)
  //  }

  /**
   * Create folder.
   *
   * Important: folderName must end with "/" to create a folder instead of a file.
   *
   * @param s3client   s3 client.
   * @param bucketName bucket name.
   * @param folderName name of folder.
   */
  def createFolder(s3client: AmazonS3, bucketName: String, folderName: String): Unit = {
    val metadata = new ObjectMetadata
    metadata.setContentLength(0)
    val emptyContent = new ByteArrayInputStream(new Array[Byte](0))
    val putObjectRequest = new PutObjectRequest(bucketName, folderName, emptyContent, metadata)
    s3client.putObject(putObjectRequest)
  }

  /**
   * Emulates folder deletion, if prefix is name of folder (sort of like full path).
   *
   * @param s3client   s3 client.
   * @param bucketName bucket name.
   * @param prefix     prefix, such as "path/to/folder".
   */
  def deletePathsWithPrefix(s3client: AmazonS3, bucketName: String, prefix: String): Unit = {
    val listObjectsRequest = new ListObjectsRequest()
      .withBucketName(bucketName)
      .withPrefix(prefix)
    var objectListing = s3client.listObjects(listObjectsRequest)

    val break = new Breaks

    break.breakable {
      while (true) {
        import scala.collection.JavaConversions._
        for (objectSummary <- objectListing.getObjectSummaries) {
          s3client.deleteObject(bucketName, objectSummary.getKey)
        }
        if (objectListing.isTruncated) objectListing = s3client.listNextBatchOfObjects(objectListing)
        else break.break()
      }
    }
  }

  /**
   * Emulates check for folder existence.
   *
   * @param s3client   s3 client.
   * @param bucketName bucket name.
   * @param prefix     prefix, such as "path/to/folder".
   * @return is there at least one file with this prefix? True or false?
   */
  def doesPathExist(s3client: AmazonS3, bucketName: String, prefix: String): Boolean = {
    s3client.listObjects(bucketName, prefix)
      .getObjectSummaries.size() > 0
  }

  /**
   * Renames all files that have the specified prefix.
   *
   * @param s3client   s3 client.
   * @param bucketName bucket name.
   * @param oldPrefix  prefix to rename.
   * @param newPrefix  new prefix.
   */
  def renamePathsWithPrefix(s3client: AmazonS3, bucketName: String, oldPrefix: String, newPrefix: String): Unit = {
    val listObjectsRequest = new ListObjectsRequest()
      .withBucketName(bucketName)
      .withPrefix(oldPrefix)
    var objectListing = s3client.listObjects(listObjectsRequest)

    val break = new Breaks

    break.breakable {
      while (true) {
        import scala.collection.JavaConversions._
        for (objectSummary <- objectListing.getObjectSummaries) {
          s3client.copyObject(bucketName, objectSummary.getKey, bucketName, objectSummary.getKey.replaceFirst(Pattern.quote(oldPrefix), newPrefix))
        }
        if (objectListing.isTruncated) objectListing = s3client.listNextBatchOfObjects(objectListing)
        else break.break()
      }
    }
    deletePathsWithPrefix(s3client, bucketName, oldPrefix)
  }
}
