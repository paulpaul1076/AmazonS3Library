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
    client.deleteBucket(bucketName)
    api.shutdown
  }

  @Test
  def createFolderIfNotExists(): Unit = {
    val folderName = "folder1"
    val doesFolderExistInitially = S3DirectoryManipulator.doesPathExist(client, bucketName, folderName)

    S3DirectoryManipulator.createFolderIfNotExists(client, bucketName, folderName)
    val doesFolderExistInTheEnd = S3DirectoryManipulator.doesPathExist(client, bucketName, folderName)

    Assert.assertTrue(!doesFolderExistInitially && doesFolderExistInTheEnd)
  }

  @Test
  def deletePathsWithPrefix(): Unit = {
    val folderName = "folder2"

    S3DirectoryManipulator.createFolderIfNotExists(client, bucketName, folderName + "/")
    val doesFolderExistInitially = S3DirectoryManipulator.doesPathExist(client, bucketName, folderName)

    S3DirectoryManipulator.deletePathsWithPrefix(client, bucketName, folderName)
    val doesFolderExistAfterDeletion = S3DirectoryManipulator.doesPathExist(client, bucketName, folderName)

    Assert.assertTrue(doesFolderExistInitially && !doesFolderExistAfterDeletion)
  }

  @Test
  def renamePathsWithPrefix(): Unit = {
    val oldName = "folder3"
    val newName = "folder4"

    val doesFolderExistInitially = S3DirectoryManipulator.doesPathExist(client, bucketName, oldName)
    S3DirectoryManipulator.createFolderIfNotExists(client, bucketName, oldName)

    S3DirectoryManipulator.renamePathsWithPrefix(client, bucketName, oldName, newName)
    val didItGetRenamed =
      !S3DirectoryManipulator.doesPathExist(client, bucketName, oldName) &&
        S3DirectoryManipulator.doesPathExist(client, bucketName, newName)

    Assert.assertTrue(!doesFolderExistInitially && didItGetRenamed)
  }

  @Test
  def doesPathExist(): Unit = {
    val folderName = "folder5"
    S3DirectoryManipulator.createFolderIfNotExists(client, bucketName, folderName)
    val doesPathExist1 = client.listObjects(bucketName, folderName).getObjectSummaries.size() > 0
    val doesPathExist2 = S3DirectoryManipulator.doesPathExist(client, bucketName, folderName)
    val doesPathNotExist = !S3DirectoryManipulator.doesPathExist(client, bucketName, "non-existent-dir")
    Assert.assertEquals(doesPathExist1, doesPathExist2)
    Assert.assertTrue(doesPathNotExist)
  }
}
