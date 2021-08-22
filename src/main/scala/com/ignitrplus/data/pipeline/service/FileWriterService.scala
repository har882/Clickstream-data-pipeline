package com.ignitrplus.data.pipeline.service

import com.ignitrplus.data.pipeline.constants.ApplicationConstants.{JDBC_DRIVER, KEY_PASSWORD,  SQL_URL_STAGING, USER_NAME}
import org.apache.spark.sql.DataFrame

object FileWriterService {

  def writeFile(df:DataFrame, writeFormat: String, path:String): Unit = {
    df.write
      .option("header",true)
      .format(writeFormat)
      .save(path)
  }
  def sqlWrite(df : DataFrame, tableName : String, url:String) : Unit = {
    val prop = new java.util.Properties
    prop.setProperty("driver", JDBC_DRIVER)
    prop.setProperty("user", USER_NAME)
    prop.setProperty("password", KEY_PASSWORD)
    df.write.mode("overwrite").jdbc(url, tableName, prop)
  }}
