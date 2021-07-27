package com.ignitrplus.data.pipeline.transform
import com.ignitrplus.data.pipeline.constants.ApplicationConstants.ig
import org.apache.spark.sql.functions.{col, lower,unix_timestamp}

object Tranformation {
  def dataType: Unit = {
    val dfModified = ig.withColumn("event_timestamp", unix_timestamp(col("event_timestamp"), "MM/dd/yyyy H:mm").cast("double").cast("timestamp"))
    dfModified.printSchema()
    dfModified.show()

  }
  def lowerCase: Unit={
    //lowercase the redirection_source column
    val df2 = ig.withColumn("redirection_source", lower(col("redirection_source")))
    df2.show()
  }

}
