package pack

import java.io.ByteArrayInputStream
import java.util.regex.Pattern

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{CopyObjectResult, ListObjectsRequest, ObjectListing, ObjectMetadata, PutObjectRequest, S3ObjectSummary}

/**
  * Utilities for manipulating directories  in s3.
  */
object S3DirectoryManipulator {
  /**
    * Create folder.
    *
    * Important: folderName must end with "/" to create a folder instead of a file.
    *
    * In the regex {{{"[^/]"}}} means any character but {{{'/'}}}.
    *
    * @param s3client   s3 client.
    * @param bucketName bucket name.
    * @param folderName name of folder.
    */
  def createFolderIfNotExists(s3client: AmazonS3, bucketName: String, folderName: String): Unit = {
    val folderRegex = "[^/]+(/[^/]+)*"
    if (!folderName.matches(folderRegex))
      throw new IllegalArgumentException("Bad folder name. Should be according to this regex: \"[^/]+(/[^/]+)*\"")

    if (doesPathExist(s3client, bucketName, folderName)) return

    val metadata = new ObjectMetadata
    metadata.setContentLength(0)
    val emptyContent = new ByteArrayInputStream(new Array[Byte](0))
    val suffix = "/"
    s3client.putObject(bucketName, folderName + suffix, emptyContent, metadata)
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
    * Emulates folder deletion, if prefix is name of folder (sort of like full path) .
    *
    * @param s3client   s3 client.
    * @param bucketName bucket name.
    * @param prefix     prefix, such as "path/to/folder".
    */
  def deletePathsWithPrefix(s3client: AmazonS3, bucketName: String, prefix: String): Unit = {
    def deleteAction(objectSummary: S3ObjectSummary): Unit = {
      s3client.deleteObject(bucketName, objectSummary.getKey)
    }

    applyActionToAllPrefixedObjects(s3client, prefix, bucketName, deleteAction)
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
    def copyAction(objectSummary: S3ObjectSummary): CopyObjectResult = {
      val key = objectSummary.getKey
      s3client.copyObject(bucketName, key, bucketName, key.replaceFirst(Pattern.quote(oldPrefix), newPrefix))
    }

    applyActionToAllPrefixedObjects(s3client, oldPrefix, bucketName, copyAction)
    deletePathsWithPrefix(s3client, bucketName, oldPrefix)
  }

  /**
    * Apply a particular action to all objects with specified prefix.
    *
    * @param s3client s3client.
    * @param prefix   prefix.
    * @param action   action.
    */
  private def applyActionToAllPrefixedObjects(s3client: AmazonS3, prefix: String, bucketName: String, action: S3ObjectSummary => Unit): Unit = {
    def applyActionRecursively(objectListing: ObjectListing): Unit = {
      import scala.collection.JavaConversions._
      for (objectSummary <- objectListing.getObjectSummaries) {
        action(objectSummary)
      }
      if (objectListing.isTruncated)
        applyActionRecursively(s3client.listNextBatchOfObjects(objectListing))
    }

    val objectListing = s3client.listObjects(bucketName, prefix)
    applyActionRecursively(objectListing)
  }
}
