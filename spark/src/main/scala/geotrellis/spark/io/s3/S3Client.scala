package geotrellis.spark.io.s3

import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.services.s3.{AmazonS3Client => AWSAmazonS3Client}
import java.io.{InputStream, ByteArrayInputStream, DataInputStream, ByteArrayOutputStream}
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.services.s3.model._
import com.typesafe.scalalogging.slf4j._
import scala.collection.JavaConverters._
import scala.util.Random
import com.amazonaws.ClientConfiguration

trait S3Client extends LazyLogging {

  def listObjects(listObjectsRequest: ListObjectsRequest): ObjectListing
  
  def listObjects(bucketName: String, prefix: String): ObjectListing =          
      listObjects(new ListObjectsRequest(bucketName, prefix, null, null, null));

  def getObject(getObjectRequest: GetObjectRequest): S3Object

  def putObject(putObjectRequest: PutObjectRequest): PutObjectResult

  def getObject(bucketName: String, key: String): S3Object = 
    getObject(new GetObjectRequest(bucketName, key))

  def putObject(bucketName: String, key: String, input: InputStream, metadata: ObjectMetadata): PutObjectResult = 
    putObject(new PutObjectRequest(bucketName, key, input, metadata))

  def putObject(bucketName: String, key: String, bytes: Array[Byte], metadata: ObjectMetadata): PutObjectResult = {    
    metadata.setContentLength(bytes.length)
    putObject(bucketName, key, new ByteArrayInputStream(bytes), metadata)
  }

  def putObject(bucketName: String, key: String, bytes: Array[Byte]): PutObjectResult =
    putObject(bucketName, key, bytes, new ObjectMetadata())


  def listObjectsIterator(bucketName: String, prefix: String, maxKeys: Int = 0): Iterator[S3ObjectSummary] =          
      listObjectsIterator(new ListObjectsRequest(bucketName, prefix, null, null, if (maxKeys == 0) null else maxKeys));

  def listObjectsIterator(request: ListObjectsRequest): Iterator[S3ObjectSummary] =
    new Iterator[S3ObjectSummary] {      
      var listing = listObjects(request)
      var iter = listing.getObjectSummaries.asScala.iterator

      def getNextPage: Boolean =  {
        val nextRequest = request.withMarker(listing.getNextMarker)
        listing = listObjects(nextRequest)
        listing.getObjectSummaries.asScala.iterator        
        iter.hasNext
      }

      def hasNext: Boolean = {
        iter.hasNext || getNextPage
      }      

      def next: S3ObjectSummary = iter.next
    }                
}

object S3Client {
  def defaultConfiguration = {
    val config = new com.amazonaws.ClientConfiguration
    config.setMaxConnections(128)
    config.setMaxErrorRetry(16)
    config.setConnectionTimeout(100000)
    config.setSocketTimeout(100000)
    config.setRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(32))
    config
  }

  def default =
    new AmazonS3Client(new DefaultAWSCredentialsProviderChain(), defaultConfiguration)
}

class AmazonS3Client(s3client: AWSAmazonS3Client) extends S3Client {

  def this(credentials: AWSCredentials, config: ClientConfiguration) =
    this(new AWSAmazonS3Client(credentials, config))

  def this(provider: AWSCredentialsProvider, config: ClientConfiguration) =
    this(new AWSAmazonS3Client(provider, config))

  def this(provider: AWSCredentialsProvider) =
    this(provider, new ClientConfiguration())

  def listObjects(listObjectsRequest: ListObjectsRequest): ObjectListing = {
    s3client.listObjects(listObjectsRequest)
  }

  def getObject(getObjectRequest: GetObjectRequest): S3Object = {
    s3client.getObject(getObjectRequest)
  }

  def putObject(putObjectRequest: PutObjectRequest): PutObjectResult = {
    s3client.putObject(putObjectRequest)
  }
}