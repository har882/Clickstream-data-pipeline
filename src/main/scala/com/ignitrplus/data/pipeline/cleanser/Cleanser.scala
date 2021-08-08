package com.ignitrplus.data.pipeline.cleanser

import com.ignitrplus.data.pipeline.constants.ApplicationConstants.WRITE_FORMAT
import com.ignitrplus.data.pipeline.service.FileWriterService
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, lower, row_number, trim, unix_timestamp, when}
import org.apache.spark.sql.types.StringType



object Cleanser {


  def dataTypeValidation(df: DataFrame, columnList: Seq[String], dataType: Seq[String],path:String): DataFrame = {
    var dfCrDataType = df
    for (i <- columnList.indices) {
      if (dataType(i) == "timestamp")
        dfCrDataType = dfCrDataType.withColumn(columnList(i), unix_timestamp(col(columnList(i)), "MM/dd/yyyy H:mm").cast("timestamp"))
      else
        dfCrDataType = dfCrDataType.withColumn(columnList(i), col(columnList(i)).cast(dataType(i)))
    }
    //dfCrDataType.printSchema()
    /**Temporary*/ FileWriterService.writeFile(dfCrDataType,WRITE_FORMAT,path)
    dfCrDataType
  }

  /**first we should filter out string columns into list and
   * use column from the filter list to trim all string columns. */
  def trimColumn(df: DataFrame,path:String): DataFrame = {
    var dfTrimColumn = df
    val stringColumns = df.schema.fields.filter(_.dataType.isInstanceOf[StringType])
    stringColumns.foreach(f=>{
      dfTrimColumn = dfTrimColumn.withColumn(f.name,trim(col(f.name)))
    })
    //dfTrimColumn.show()
    /**Temporary*/ FileWriterService.writeFile(dfTrimColumn,WRITE_FORMAT,path)
    dfTrimColumn
  }


  /** This function does two jobs:
   * 1. check Null row for key Column and filter out it out
   * 2. if null row found then write it in a separate file
   * Parameter :- df: dataframe , columnList: seq of primary key , path: path to write null rows */
  def checkNFilterNullRow(df:DataFrame, columnList: Seq[String],path:String): DataFrame = {

    val columnNames:Seq[Column] = columnList.map(ex => col(ex))
    val condition:Column = columnNames.map(ex => ex.isNull).reduce(_||_)
    val dfCheckNullKeyRows:DataFrame = df.withColumn("nullFlag" , when(condition,value = "true").otherwise(value = "false"))
    dfCheckNullKeyRows.show()

    /** filter out all Null row in a dataframe,say dfNullRows */
    val  dfNullRows:DataFrame = dfCheckNullKeyRows.filter(dfCheckNullKeyRows("nullFlag")==="true")

    /** if null rows are preset in dfNullRows then write it in a separate file */
    if (dfNullRows.count() > 0)
      FileWriterService.writeFile(dfNullRows,WRITE_FORMAT,path)
    dfCheckNullKeyRows
  }


  /** filtering out not null Row to further use it into our pipeline*/
  def filterNotNullRow(df: DataFrame, columnList: Seq[String],path:String): DataFrame = {
    val dfFilterNotNullRow = df.na.drop(columnList)
    /**Temporary*/ FileWriterService.writeFile(dfFilterNotNullRow,WRITE_FORMAT,path)
    dfFilterNotNullRow
  }

  /** remove duplicates for List of column */
  def removeDuplicate(df: DataFrame, columnList: Seq[String],path:String): DataFrame = {

    val winSpec = Window.partitionBy(columnList.map(col): _*).orderBy(desc("event_timestamp"))
    val primaryData: DataFrame = df.withColumn("row_num", row_number().over(winSpec))
    val dfRemoveDuplicate: DataFrame = primaryData.filter("row_num == 1").drop("row_num").repartition(columnList.map(col): _*)

    /**Temporary*/FileWriterService.writeFile(dfRemoveDuplicate,WRITE_FORMAT,path)
    dfRemoveDuplicate
  }

  /** covert Seq of column into lowercase  */
  def convertToLowerCase(df: DataFrame, columnList: Seq[String],path:String): DataFrame = {
    var dfConvertToLowerCase = df
    for (n <- columnList) dfConvertToLowerCase = df.withColumn(n, lower(col(n)))


    /**Temporary*/FileWriterService.writeFile(dfConvertToLowerCase,WRITE_FORMAT,path)
    dfConvertToLowerCase
  }

}
