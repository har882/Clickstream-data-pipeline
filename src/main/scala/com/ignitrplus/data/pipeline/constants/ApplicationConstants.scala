package com.ignitrplus.data.pipeline.constants

import com.ignitrplus.data.pipeline.service.FileReadearService.readFile
import com.ignitrplus.data.pipeline.util.ApplicationUtil.createSparkSession
import org.apache.spark.sql.DataFrame

object ApplicationConstants {

  var filePath = "data/input/clickstream/clickstream_log.csv"
  var ig: DataFrame =readFile("filePath")(spark= createSparkSession("product","local"))
}
