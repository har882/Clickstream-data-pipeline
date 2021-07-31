package com.ignitrplus.data.pipeline.service

import com.ignitrplus.data.pipeline.constants.ApplicationConstants._
import com.ignitrplus.data.pipeline.cleanser.Clenser
import org.apache.spark.sql.SparkSession

object PipelineService {
  def execute()(implicit spark: SparkSession): Unit = {

    /** read dataset */
    val dfClickStream = FileReaderService.readFile(CLICKSTREAM_DATASET,READ_FORMAT).drop("id")
    val dfItem = FileReaderService.readFile(ITEM_DATASET,READ_FORMAT)

    /**change datatype */
    val dfDataType1 = Clenser.changeDataType(dfClickStream, COL_NAME_DATATYPE_CLICKSTREAM,NEW_DATATYPE_CLICKSTREAM)
    val dfDataType2 = Clenser.changeDataType(dfItem, COL_NAME_DATATYPE_ITEM,NEW_DATATYPE_ITEM)

    /**check and filter null row */
    val dfNullCol1 = Clenser.checkNFilterNullRow(dfDataType1, COL_NAME_PRIMARY_KEY_CLICKSTREAM)
    val dfNullCol2 = Clenser.checkNFilterNullRow(dfDataType2, COL_NAME_PRIMARY_KEY_ITEM)

    /**filtering out not null Row */
    val dfNotNullCol1 = Clenser.filterNotNullRow(dfDataType1, COL_NAME_PRIMARY_KEY_CLICKSTREAM)
    val dfNotNullCol2 = Clenser.filterNotNullRow(dfDataType2, COL_NAME_PRIMARY_KEY_ITEM)

    /**remove duplicate */
    val dfNoDuplicate1 = Clenser.removeDuplicate(dfDataType1, COL_NAME_PRIMARY_KEY_CLICKSTREAM)
    //val dfNoDuplicate2 = Clenser.removeDuplicate(dfDataType2, COL_NAME_NULLKEY_DF2)

    /**convert to lowercase */
    val dfLowerCase1 = Clenser.convertToLowerCase(dfNoDuplicate1,COL_NAME_LOWERCASE_CLICKSTREAM)
    val dfLowerCase2 = Clenser.convertToLowerCase(dfNotNullCol2,COL_NAME_LOWERCASE_ITEM)


  }
}
