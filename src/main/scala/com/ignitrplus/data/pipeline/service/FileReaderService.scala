package com.ignitrplus.data.pipeline.service

import com.ignitrplus.data.pipeline.constants.ApplicationConstants.{JDBC_DRIVER, KEY_PASSWORD, USER_NAME, WRITE_FORMAT}
import com.ignitrplus.data.pipeline.exception.ExceptionFile.InvalidInputFileException
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileReaderService {

  def readFile(path: String, readformat: String, writePath: String)(implicit spark: SparkSession): DataFrame = {


    val dfRead = spark.read.format(readformat)
      .option("inferSchema", "true")
      .option("header", "true")
      .load(path)
    if (dfRead.count == 0) {
      throw new InvalidInputFileException("The file chosen is empty, Please choose another file.")
    }
    dfRead


  }

  def sqlRead( tableName : String, url:String)(implicit spark: SparkSession) : DataFrame = {
    val prop = new java.util.Properties
    prop.setProperty("driver", JDBC_DRIVER)
    prop.setProperty("user", USER_NAME)
    prop.setProperty("password", KEY_PASSWORD)
    spark.read.jdbc(url, tableName, prop)
  }

}

