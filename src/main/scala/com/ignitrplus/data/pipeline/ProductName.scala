package com.ignitrplus.data.pipeline


import com.ignitrplus.data.pipeline.constants.ApplicationConstants.{APP_NAME, MASTER}
import com.ignitrplus.data.pipeline.service.PipelineService
import com.ignitrplus.data.pipeline.util.ApplicationUtil
import org.apache.spark.sql.SparkSession

object ProductName {
  def main(args: Array[String]): Unit = {

    //Creating a SparkSession
    implicit val spark: SparkSession = ApplicationUtil.createSparkSession(APP_NAME, MASTER)


      PipelineService.execute()



  }

}
