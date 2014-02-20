/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.metrics.sink


import java.util.Properties
import com.codahale.metrics.MetricRegistry
import com.plausiblelabs.metrics.reporting.CloudWatchReporter
import com.amazonaws.auth.BasicAWSCredentials
import java.util.concurrent.TimeUnit.SECONDS
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.AmazonClientException
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider
import org.apache.spark.Logging
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient


class CloudwatchSink(val property: Properties, val registry: MetricRegistry) extends Sink with Logging {
 
    val reporter = new CloudWatchReporter.Builder(registry,"spark",new AmazonCloudWatchClient(getCredentials())).withEC2InstanceIdDimension().build()
 
  override def start() {
         reporter.start(60, SECONDS)
  }

  override def stop() {
    reporter.stop()
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
	       credentialsProvider 
     }


}

