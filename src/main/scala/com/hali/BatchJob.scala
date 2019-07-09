/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hali

import com.hali.CaseClasses.Event
import com.hali.Transformations._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.util.StringUtils
import org.slf4j.{Logger, LoggerFactory}

/**
  * Skeleton for a Flink Batch Job.
  *
  * For a tutorial how to write a Flink batch application, check the
  * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
  *
  * To package your application into a JAR file for execution,
  * change the main class in the POM.xml file to this class (simply search for 'mainClass')
  * and run 'mvn clean package' on the command line.
  */
object BatchJob {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    // set up the batch execution environment

    val parameter = ParameterTool.fromArgs(args)

    val env = ExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setGlobalJobParameters(parameter)

    val input = parameter.get("input")
    val outputPath = parameter.get("output")
    if (StringUtils.isNullOrWhitespaceOnly(input) || StringUtils.isNullOrWhitespaceOnly(outputPath) || input.equals("__NO_VALUE_KEY") || outputPath.equals("__NO_VALUE_KEY")) {
      logger.error("Parameter error! Job needs both of input and output path")
      sys.exit(-1)
    }
    logger.info("Input file name {}", input)
    logger.info("Output path {}", outputPath)


    val allEvents: DataSet[Event] = env.readCsvFile[Event](input
      , fieldDelimiter = "|"
      , ignoreFirstLine = true)

    //Unique Product View counts by ProductId
    getUniqueProductViewCount(allEvents).writeAsCsv(outputPath + "/unique_product_view.txt", fieldDelimiter = "|")

    // Unique Event counts
    getUniqueEventCount(allEvents).writeAsCsv(outputPath + "/unique_event.txt", fieldDelimiter = "|")

    //Top 5 Users who fulfilled all the events (view,add,remove,click)
    getTopFiveUsersFulfilledAllEvents(allEvents).writeAsText(outputPath + "/fulfilled.txt")


    val filteredUser: DataSet[Event] = allEvents.filter(line => line.userId == 47)

    //All events of #UserId : 47
    getUserEvent(filteredUser).writeAsCsv(outputPath + "/user_event_count.txt", fieldDelimiter = "|").setParallelism(1)

    //Product Views of #UserId : 47
    getUserProductView(filteredUser).writeAsText(outputPath + "/user_product_view.txt").setParallelism(1)


    // execute program
    env.execute("Flink Batch Scala API Skeleton")
  }
}
