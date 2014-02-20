package org.apache.spark.streaming.examples

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.KinesisInputDStream
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.DescribeStreamRequest
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider
import scala.collection.JavaConversions._
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.AmazonClientException
import org.apache.spark.Logging



object KinesisWordCount extends Logging {
  def main(args: Array[String]) {
	  	
	if (args.length < 2) {
		System.err.println("Usage: KinesisWordCount <master> <streamname>\n" +
		"In local mode, <master> should be 'local[n]' with n > 1")
		System.exit(1)
	}
	
	
	val ssc = new StreamingContext(args(0), "KinesisWordCount", Seconds(30),System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES")))
	
	var canalClient  = new AmazonKinesisClient(getCredentials());
  
	val describeStreamRequest = new DescribeStreamRequest().withStreamName(args(1));
	val describeStreamResult = canalClient.describeStream(describeStreamRequest);
	val shards = describeStreamResult.getStreamDescription().getShards().toList

	val stream = ssc.union(for (shard <- shards) yield (ssc.networkStream[String](new KinesisInputDStream("none",args(1),shard.getShardId()))))
	


        val hashTags = stream.flatMap(status => status.split(" "))

        val wordCount = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                     .map{case (topic, count) => (count, topic)}
               
	wordCount.print
	ssc.start()

  }
  def getCredentials():AWSCredentialsProvider = {
    
		  var credentialsProvider:AWSCredentialsProvider=null
    	  try {
	            credentialsProvider = new InstanceProfileCredentialsProvider()
	            // Verify we can fetch credentials from the provider
	            credentialsProvider.getCredentials()   
	            logInfo("Obtained credentials from the IMDS.")
	        } catch  {
	          case e:AmazonClientException => {         
	             credentialsProvider = new ClasspathPropertiesFileCredentialsProvider()
	            // Verify we can fetch credentials from the provider
	            credentialsProvider.getCredentials()  
	            logInfo("Obtained credentials from the properties file.")
	          }
	        }
               logInfo("Using credentials with access key id: " + credentialsProvider.getCredentials().getAWSAccessKeyId());
	       credentialsProvider 
  }
}
