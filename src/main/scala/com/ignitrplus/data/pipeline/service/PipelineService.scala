package com.ignitrplus.data.pipeline.service

import com.ignitrplus.data.pipeline.constants.ApplicationConstants._
import com.ignitrplus.data.pipeline.cleanser.Cleanser
import org.apache.spark.sql.SparkSession

object PipelineService {
  def execute()(implicit spark: SparkSession): Unit = {

    /** read dataset */
    val dfClickStream = FileReaderService.readFile(CLICKSTREAM_DATASET,READ_FORMAT).drop("id")
    val dfItem = FileReaderService.readFile(ITEM_DATASET,READ_FORMAT)

    /**change datatype */
    val dfDataType1 = Cleanser.changeDataType(dfClickStream, COL_NAME_DATATYPE_CLICKSTREAM,NEW_DATATYPE_CLICKSTREAM)
    val dfDataType2 = Cleanser.changeDataType(dfItem, COL_NAME_DATATYPE_ITEM,NEW_DATATYPE_ITEM)

    /**check and filter null row */
    val dfNullCol1 = Cleanser.checkNFilterNullRow(dfDataType1, COL_NAME_PRIMARY_KEY_CLICKSTREAM)
    val dfNullCol2 = Cleanser.checkNFilterNullRow(dfDataType2, COL_NAME_PRIMARY_KEY_ITEM)

    /**filtering out not null Row */
    val dfNotNullCol1 = Cleanser.filterNotNullRow(dfDataType1, COL_NAME_PRIMARY_KEY_CLICKSTREAM)
    val dfNotNullCol2 = Cleanser.filterNotNullRow(dfDataType2, COL_NAME_PRIMARY_KEY_ITEM)

    /**remove duplicate */
    val dfNoDuplicate1 = Cleanser.removeDuplicate(dfDataType1, COL_NAME_PRIMARY_KEY_CLICKSTREAM)
    //val dfNoDuplicate2 = Clenser.removeDuplicate(dfDataType2, COL_NAME_NULLKEY_DF2)

    /**convert to lowercase */
    val dfLowerCase1 = Cleanser.convertToLowerCase(dfNoDuplicate1,COL_NAME_LOWERCASE_CLICKSTREAM)
    val dfLowerCase2 = Cleanser.convertToLowerCase(dfNotNullCol2,COL_NAME_LOWERCASE_ITEM)


  }
}
