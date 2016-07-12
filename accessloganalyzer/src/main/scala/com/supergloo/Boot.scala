package com.supergloo

import com.supergloo.utils.AccessLogParser
import com.supergloo.models._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://www.supergloo.com
  */
object Boot {

  import utils.Utils._

 def main(args: Array[String]): Unit = {

   val sparkConf = new SparkConf(true)
     .setMaster("local[2]")
     .setAppName("SparkAnalyzer")

   val sparkContext = new SparkContext(sparkConf)

   /**
     * Defining list of all HTTP status codes divided into status groups
     * This list is read only, and it is used for parsing access log file in order to count status code groups
     * This example of broadcast variable shows how broadcast value can be used in computations on workers nodes
     */
   val httpStatusList = sparkContext broadcast populateHttpStatusList

   /**
     * Definition of accumulators for counting specific HTTP status codes
     * Accumulator variable is used because of all the updates to this variable in every executor is relayed back to the driver.
     * Otherwise they are local variable on executor and it is not relayed back to driver
     * so driver value is not changed
     */
   val httpInfo = sparkContext accumulator(0, "HTTP 1xx")
   val httpSuccess = sparkContext accumulator(0, "HTTP 2xx")
   val httpRedirect = sparkContext accumulator(0, "HTTP 3xx")
   val httpClientError = sparkContext accumulator(0, "HTTP 4xx")
   val httpServerError = sparkContext accumulator(0, "HTTP 5xx")

   /**
     * Iterate over access.log file and parse every line
     * for every line extract HTTP status code from it and update appropriate accumulator variable
     */
   sparkContext.textFile(getClass.getResource("/access.log").getPath, 2).foreach { line =>
     httpStatusList.value foreach {
       case httpInfoStatus: HttpInfoStatus if (AccessLogParser.parseHttpStatusCode(line).equals(Some(httpInfoStatus))) => httpInfo += 1
       case httpSuccessStatus: HttpSuccessStatus if (AccessLogParser.parseHttpStatusCode(line).equals(Some(httpSuccessStatus))) => httpSuccess += 1
       case httpRedirectStatus: HttpRedirectStatus if (AccessLogParser.parseHttpStatusCode(line).equals(Some(httpRedirectStatus))) => httpRedirect += 1
       case httpClientErrorStatus: HttpClientErrorStatus if (AccessLogParser.parseHttpStatusCode(line).equals(Some(httpClientErrorStatus))) => httpClientError += 1
       case httpServerErrorStatus: HttpServerErrorStatus if (AccessLogParser.parseHttpStatusCode(line).equals(Some(httpServerErrorStatus))) => httpServerError += 1
       case _ =>
     }
   }

   println("########## START ##########")
   println("Printing HttpStatusCodes result from parsing access log")
   println(s"HttpStatusInfo : ${httpInfo.value}")
   println(s"HttpStatusSuccess : ${httpSuccess.value}")
   println(s"HttpStatusRedirect : ${httpRedirect.value}")
   println(s"HttpStatusClientError : ${httpClientError.value}")
   println(s"HttpStatusServerError : ${httpServerError.value}")
   println("########## END ##########")

   sparkContext.stop()
 }

}
