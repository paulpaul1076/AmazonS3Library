package pack

import com.amazonaws.services.s3.AmazonS3
import org.junit.{Assert, Test}
import org.scalatest.junit.JUnitSuite

/**
  * Tests S3DirectoryManipulator.
  */
class S3DirectoryManipulatorTest extends JUnitSuite {

  var client: AmazonS3 = new AmazonS3Mock
  val bucketName = "testbucket"

  @Test
  def createFolderIfNotExists(): Unit = {
    val amazonS3Mock = new AmazonS3Mock

    val folderName = "folder1"
    val doesFolderExistInitially = S3DirectoryManipulator.doesPathExist(amazonS3Mock, bucketName, folderName)

    S3DirectoryManipulator.createFolderIfNotExists(amazonS3Mock, bucketName, folderName)
    val doesFolderExistInTheEnd = S3DirectoryManipulator.doesPathExist(amazonS3Mock, bucketName, folderName)

    Assert.assertTrue(!doesFolderExistInitially && doesFolderExistInTheEnd)
  }

  @Test
  def deletePathsWithPrefix(): Unit = {
    val folderName = "folder2"

    S3DirectoryManipulator.createFolderIfNotExists(client, bucketName, folderName)
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
    val amazonS3Mock = new AmazonS3Mock
    val folderName = "folder5"
    S3DirectoryManipulator.createFolderIfNotExists(amazonS3Mock, bucketName, folderName)
    val doesPathExist1 = amazonS3Mock.listObjects(bucketName, folderName).getObjectSummaries.size() > 0
    val doesPathExist2 = S3DirectoryManipulator.doesPathExist(amazonS3Mock, bucketName, folderName)
    val doesPathNotExist = !S3DirectoryManipulator.doesPathExist(amazonS3Mock, bucketName, "non-existent-dir")
    Assert.assertEquals(doesPathExist1, doesPathExist2)
    Assert.assertTrue(doesPathNotExist)
  }
}
