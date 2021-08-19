package com.ignitrplus.data.pipeline


import com.ignitrplus.data.pipeline.constants.ApplicationConstants
import com.ignitrplus.data.pipeline.constants.ApplicationConstants.{APP_NAME, MASTER}
import com.ignitrplus.data.pipeline.exception.ExceptionFile.{InvalidInputFileException, NullValuesException, UnMatchedItemIdException}
import com.ignitrplus.data.pipeline.service.PipelineService
import com.ignitrplus.data.pipeline.util.ApplicationUtil
import org.apache.spark.sql.{AnalysisException, SparkSession}


object ClickstreamDataPipeline {
  def main(args: Array[String]): Unit = {

    //Creating a SparkSession
    implicit val spark: SparkSession = ApplicationUtil.createSparkSession(APP_NAME, MASTER)


    try {
      PipelineService.executePipeline()
    } catch {
      case ex: InvalidInputFileException =>
        println(ex + "The file chosen is empty, Please choose another file.")
        sys.exit(ApplicationConstants.FAILURE_EXIT_CODE)
//      case ex: AnalysisException =>
//        println(ex + " Query fails to analyze")
//        sys.exit(ApplicationConstants.FAILURE_EXIT_CODE)
      case ex: IndexOutOfBoundsException =>
        println(ex + "Index out of bound")
        sys.exit(ApplicationConstants.FAILURE_EXIT_CODE)
      case ex: NullValuesException =>
        println(ex + "Null Values Found")
        sys.exit(ApplicationConstants.FAILURE_EXIT_CODE)
      case ex: UnMatchedItemIdException =>
        println(ex + "There is an unmatched item_id in clickstream dataset")
        sys.exit(ApplicationConstants.FAILURE_EXIT_CODE)
//      case ex: Exception =>
//        println(ex + " Unknown Exception has occurred")
//        sys.exit(ApplicationConstants.FAILURE_EXIT_CODE)
    } finally {
      println("Finally Exiting...")
    }




  }

}
