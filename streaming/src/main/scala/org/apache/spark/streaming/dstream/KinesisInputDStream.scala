package org.apache.spark.streaming.dstream

import org.apache.spark.storage.StorageLevel
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.Socket
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest
import com.amazonaws.services.kinesis.model.GetShardIteratorResult
import scala.collection.JavaConversions._
import java.nio.charset.Charset
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.GetRecordsRequest
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.AmazonClientException
import com.amazonaws.auth.AWSCredentialsProvider


class KinesisInputDStream(apiUrl:String,streamName:String,shard:String) extends NetworkReceiver[String]{
  
    	 protected lazy val blocksGenerator: BlockGenerator = new BlockGenerator(StorageLevel.MEMORY_ONLY_SER_2)
    
         protected def onStart() = {
      
	         val decoder = Charset.forName("UTF-8").newDecoder();
	            
	        blocksGenerator.start()
	        
	      
	           
	        var canalClient = new AmazonKinesisClient(getCredentials());
			
			var si = new GetShardIteratorRequest().withShardId(shard).withShardIteratorType("LATEST").withStreamName(streamName);
			var siRes = canalClient.getShardIterator(si);
			var nr = new GetRecordsRequest().withShardIterator(siRes.getShardIterator());
			var gnr = canalClient.getRecords(nr);
			var records = gnr.getRecords().toList;
			while(true){
				records.foreach(rec=>{
				  blocksGenerator += decoder.decode(rec.getData()).toString()
				})
				gnr = canalClient.getRecords(new GetRecordsRequest().withShardIterator(gnr.getNextShardIterator()))
				records = gnr.getRecords().toList
			}	
          
      }

     protected def onStop() {
       blocksGenerator.stop()
     }
     private def getCredentials():AWSCredentialsProvider = {
    
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
