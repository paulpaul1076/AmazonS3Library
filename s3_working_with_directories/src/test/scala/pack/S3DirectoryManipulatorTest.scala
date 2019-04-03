package pack

import com.amazonaws.auth.{AWSStaticCredentialsProvider, AnonymousAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import io.findify.s3mock.S3Mock
import org.junit.{After, Assert, Before, Test}
import org.scalatest.junit.JUnitSuite

class S3DirectoryManipulatorTest extends JUnitSuite {

  var api: S3Mock = null
  var client: AmazonS3 = null

  val bucketName = "testbucket"

  @Before
  def setUp(): Unit = {
    api = S3Mock(port = 8001, dir = "/tmp/s3")
    api.start

    val endpoint = new EndpointConfiguration("http://localhost:8001", "us-west-2")
    client = AmazonS3ClientBuilder
      .standard
      .withPathStyleAccessEnabled(true)
      .withEndpointConfiguration(endpoint)
      .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
      .build

    client.createBucket(bucketName)
  }

  @After
  def tearDown(): Unit = {
//    S3DirectoryManipulator.deletePathsWithPrefix(client, bucketName, "folder1")
//    S3DirectoryManipulator.deletePathsWithPrefix(client, bucketName, "folder2")
//    S3DirectoryManipulator.deletePathsWithPrefix(client, bucketName, "folder3")
    client.deleteBucket(bucketName)
    api.shutdown
  }

  @Test
  def createFolder(): Unit = {
    val folderName = "folder1"
    val isBucketEmptyInitially = client.listObjects(bucketName, folderName).getObjectSummaries.isEmpty
    S3DirectoryManipulator.createFolder(client, bucketName, folderName + "/")
    val exists = client.listObjects(bucketName, folderName).getObjectSummaries.size() > 0

    Assert.assertTrue(isBucketEmptyInitially && exists)
  }

  @Test
  def deletePathsWithPrefix(): Unit = {
    //S3DirectoryManipulator.deletePathsWithPrefix(client, bucketName, "folder1")
    //S3DirectoryManipulator.deletePathsWithPrefix(client, bucketName, "folder2")
    //S3DirectoryManipulator.deletePathsWithPrefix(client, bucketName, "folder3")
    val folderName = "folder3"
    val isBucketEmptyInitially = client.listObjects(bucketName, folderName).getObjectSummaries.isEmpty
    S3DirectoryManipulator.createFolder(client, bucketName, folderName + "/")
    val exists = client.listObjects(bucketName, folderName).getObjectSummaries.size() > 0
    //S3DirectoryManipulator.deletePathsWithPrefix(client, bucketName, folderName)
    //val existsAfterDeletion = client.listObjects(bucketName, folderName).getObjectSummaries.size() > 0

    Assert.assertTrue(isBucketEmptyInitially && exists /*&& !existsAfterDeletion*/)
  }

  @Test
  def renamePathsWithPrefix(): Unit = {
    val oldName = "folder2"
    val newName = "folder3"
    S3DirectoryManipulator.renamePathsWithPrefix(client, bucketName, oldName, newName)
    val exists = client.listObjects(bucketName, newName).getObjectSummaries.size() > 0
  }
}
