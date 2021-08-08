package com.ignitrplus.data.pipeline.service

import com.ignitrplus.data.pipeline.constants.ApplicationConstants._
import com.ignitrplus.data.pipeline.cleanser.Cleanser
import org.apache.spark.sql.SparkSession

object PipelineService {
  def executePipeline()(implicit spark: SparkSession): Unit = {

    /** read dataset */
    val dfClickStream = FileReaderService.readFile(CLICKSTREAM_DATASET,READ_FORMAT).drop("id")
    val dfItem = FileReaderService.readFile(ITEM_DATASET,READ_FORMAT)


    /**change datatype */
    val dfDataTypeClickStream = Cleanser.changeDataType(dfClickStream, COL_NAME_DATATYPE_CLICKSTREAM,NEW_DATATYPE_CLICKSTREAM)
    val dfDataTypeItem = Cleanser.changeDataType(dfItem, COL_NAME_DATATYPE_ITEM,NEW_DATATYPE_ITEM)

    /** trim */
    val dfTrimClickStream = Cleanser.trimColumn(dfDataTypeClickStream)
    val dfTrimItem = Cleanser.trimColumn(dfDataTypeItem)

    /**check and filter null row */
    val dfNullColClickStream = Cleanser.checkNFilterNullRow(dfDataTypeClickStream, COL_NAME_PRIMARY_KEY_CLICKSTREAM,CLICKSTREAM_NULL_ROWS_DATASET)
    val dfNullColItem = Cleanser.checkNFilterNullRow(dfDataTypeItem, COL_NAME_PRIMARY_KEY_ITEM,ITEM_NULL_ROWS_DATASET)

    /**filtering out not null Row */
    val dfNotNullColClickStream = Cleanser.filterNotNullRow(dfDataTypeClickStream, COL_NAME_PRIMARY_KEY_CLICKSTREAM)
    val dfNotNullColItem = Cleanser.filterNotNullRow(dfDataTypeItem, COL_NAME_PRIMARY_KEY_ITEM)

    /**remove duplicate */
    val dfNoDuplicateClickStream = Cleanser.removeDuplicate(dfDataTypeClickStream, COL_NAME_PRIMARY_KEY_CLICKSTREAM)
    //val dfNoDuplicateItem = Cleanser.removeDuplicate(dfDataTypeItem, COL_NAME_PRIMARY_KEY_ITEM)

    /**convert to lowercase */
    val dfLowerCaseClickStream = Cleanser.convertToLowerCase(dfNoDuplicateClickStream,COL_NAME_LOWERCASE_CLICKSTREAM)
    val dfLowerCaseItem = Cleanser.convertToLowerCase(dfNotNullColItem,COL_NAME_LOWERCASE_ITEM)


  }
}
