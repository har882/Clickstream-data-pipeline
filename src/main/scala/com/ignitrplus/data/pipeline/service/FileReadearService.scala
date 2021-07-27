package com.ignitrplus.data.pipeline.service
import com.ignitrplus.data.pipeline.constants.ApplicationConstants.filePath
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileReadearService {

  def readFile(path:String)(implicit spark:SparkSession): DataFrame ={

    val df= spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filePath)
    return df
  }

}

