package com.ignitrplus.data.pipeline.service

import com.ignitrplus.data.pipeline.constants.ApplicationConstants._
import com.ignitrplus.data.pipeline.cleanser.Cleanser
import org.apache.spark.sql.SparkSession

object PipelineService {
  def executePipeline()(implicit spark: SparkSession): Unit = {

    /** read dataset */
    val dfClickStream = FileReaderService.readFile(CLICKSTREAM_DATASET,READ_FORMAT,CLICKSTREAM_FILE_READER_DATASET).drop("id")
    val dfItem = FileReaderService.readFile(ITEM_DATASET,READ_FORMAT,ITEM_FILE_READER_DATASET)


    /**change datatype */
    val dfDataTypeClickStream = Cleanser.dataTypeValidation(dfClickStream,COLUMNS_VALID_DATATYPE_CLICKSTREAM,NEW_DATATYPE_CLICKSTREAM,CLICKSTREAM_DATA_TYPE_DATASET)
    val dfDataTypeItem = Cleanser.dataTypeValidation(dfItem,COLUMNS_VALID_DATATYPE_ITEM,NEW_DATATYPE_ITEM,ITEM_DATA_TYPE_DATASET)

    /** trim */
    val dfTrimClickStream = Cleanser.trimColumn(dfDataTypeClickStream,CLICKSTREAM_TRIM_DATASET)
    val dfTrimItem = Cleanser.trimColumn(dfDataTypeItem,ITEM_TRIM_DATASET)

    /**check and filter null row and return clean dataframe */
    val dfNotNullColClickStream = Cleanser.checkNFilterNullRow(dfTrimClickStream, COLUMNS_PRIMARY_KEY_CLICKSTREAM,CLICKSTREAM_NULL_ROWS_DATASET_PATH,CLICKSTREAM_NOT_NULL_DATASET)
    val dfNotNullColItem = Cleanser.checkNFilterNullRow(dfTrimItem, COLUMNS_PRIMARY_KEY_ITEM,ITEM_NULL_ROWS_DATASET_PATH,ITEM_NOT_NULL_DATASET)

    /**remove duplicate */
    val dfNoDuplicateClickStream = Cleanser.removeDuplicate(dfNotNullColClickStream, COLUMNS_PRIMARY_KEY_CLICKSTREAM,CLICKSTREAM_DEDUPLICATE_DATASET)
    //val dfNoDuplicateItem = Cleanser.removeDuplicate(dfDataTypeItem, COLUMNS_PRIMARY_KEY_ITEM,ITEM_DEDUPLICATE_DATASET)

    /**convert to lowercase */
    val dfLowerCaseClickStream = Cleanser.convertToLowerCase(dfNoDuplicateClickStream,COLUMNS_LOWERCASE_CLICKSTREAM,CLICKSTREAM_LOWERCASE_DATASET)
    val dfLowerCaseItem = Cleanser.convertToLowerCase(dfNotNullColItem,COLUMNS_LOWERCASE_ITEM,ITEM_LOWERCASE_DATASET)
  }
}
