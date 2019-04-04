package pack

import com.amazonaws.services.s3.model.{CopyObjectRequest, ObjectListing, PutObjectRequest}
import com.amazonaws.services.s3.{AmazonS3, model}
import org.junit.{Assert, Before, Test}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.junit.JUnitSuite

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Tests S3DirectoryManipulator.
  */
class S3DirectoryManipulatorTest extends JUnitSuite {

  var client: AmazonS3 = _
  val bucketName = "testbucket"
  var hashSet: mutable.Set[String] = new mutable.HashSet[String]

  @Before
  def setUp() : Unit = {
    client = mock(classOf[AmazonS3])

    when(client.listObjects(anyString(), anyString())).thenAnswer(new Answer[ObjectListing]{
      override def answer(invocation: InvocationOnMock): ObjectListing = {
        val prefix = invocation.getArgument(1).asInstanceOf[String]

        val listingMock = mock(classOf[ObjectListing])

        val summaries = hashSet.filter(_.startsWith(prefix))
          .map(key => {
            val summary = new model.S3ObjectSummary()
            summary.setKey(key)
            summary
          }).toList.asJava

        when(listingMock.getObjectSummaries).thenReturn(summaries)
        when(listingMock.isTruncated).thenReturn(false)

        listingMock
      }
    })

    when(client.putObject(anyString(), anyString(), any(), any())).thenAnswer(new Answer[PutObjectRequest]{
      override def answer(invocation: InvocationOnMock): PutObjectRequest = {
        val key = invocation.getArgument(1).asInstanceOf[String]
        hashSet.add(key)
        null
      }
    })

    when(client.copyObject(anyString(), anyString(), anyString(), anyString())).thenAnswer(new Answer[CopyObjectRequest]{
      override def answer(invocation: InvocationOnMock): CopyObjectRequest = {
        val destinationKey = invocation.getArgument(3).asInstanceOf[String]
        hashSet.add(destinationKey)
        null
      }
    })

    when(client.deleteObject(anyString(), anyString())).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        val key = invocation.getArgument(1).asInstanceOf[String]
        hashSet = hashSet.filter(!_.startsWith(key))
      }
    })
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
    val folderName = "folder5"
    S3DirectoryManipulator.createFolderIfNotExists(client, bucketName, folderName)
    val doesPathExist1 = client.listObjects(bucketName, folderName).getObjectSummaries.size() > 0
    val doesPathExist2 = S3DirectoryManipulator.doesPathExist(client, bucketName, folderName)
    val doesPathNotExist = !S3DirectoryManipulator.doesPathExist(client, bucketName, "non-existent-dir")
    Assert.assertEquals(doesPathExist1, doesPathExist2)
    Assert.assertTrue(doesPathNotExist)
  }
}
