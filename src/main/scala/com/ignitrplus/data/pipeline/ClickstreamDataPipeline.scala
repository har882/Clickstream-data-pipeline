package com.ignitrplus.data.pipeline


import com.ignitrplus.data.pipeline.constants.ApplicationConstants.{APP_NAME, MASTER}
import com.ignitrplus.data.pipeline.service.PipelineService
import com.ignitrplus.data.pipeline.util.ApplicationUtil
import org.apache.spark.sql.{AnalysisException, SparkSession}

import java.io.FileNotFoundException

object ClickstreamDataPipeline {
  def main(args: Array[String]): Unit = {

    //Creating a SparkSession
    implicit val spark: SparkSession = ApplicationUtil.createSparkSession(APP_NAME, MASTER)


    try {
      PipelineService.executePipeline()
    } catch {
      case ex: FileNotFoundException => {
        println("Exception! Missing file exception")
      }
      case ex: AnalysisException => {
        println("Exception! File not found")
      }
      case ex: IndexOutOfBoundsException => {
        println("Exception! Index out of bound")
      }
//      case ex: Exception => {
//        println("Exception! Unknown Exception has occurred")
//      }
    } finally {
      println("Finally Exiting...")
    }




  }

}
