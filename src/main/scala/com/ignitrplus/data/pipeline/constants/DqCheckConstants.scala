package com.ignitrplus.data.pipeline.constants

import com.ignitrplus.data.pipeline.cleanser.Cleanser
import com.ignitrplus.data.pipeline.constants.ApplicationConstants.{COLUMNS_LOWERCASE_CLICKSTREAM, COLUMNS_LOWERCASE_ITEM}
import org.apache.spark.sql.SparkSession

object DqCheckConstants {
  val SESSION_ID: String = "session_id"
  val ITEM_ID: String = "item_id"
  val REDIRECTION_SOURCE: String = "redirection_source"
  val VISITOR_ID: String = "visitor_id"
  val COLUMNS_CHECK_NULL_CLICKSTREAM: Seq[String] = Seq(ApplicationConstants.SESSION_ID, ApplicationConstants.ITEM_ID,ApplicationConstants.VISITOR_ID)



}
