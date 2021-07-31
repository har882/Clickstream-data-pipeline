package com.ignitrplus.data.pipeline.service

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.FileNotFoundException
import scala.util.{Failure, Success, Try}

object FileReaderService {

  def readFile(path: String,readformat: String)(implicit spark: SparkSession): DataFrame = {

      val dfRead = spark.read.format(readformat)
        .option("inferSchema", "true")
        .option("header", "true")
        .load(path)
    dfRead


  }

}

