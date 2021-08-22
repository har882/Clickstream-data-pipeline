package com.ignitrplus.data.pipeline.service

import com.ignitrplus.data.pipeline.constants.ApplicationConstants._
import com.ignitrplus.data.pipeline.cleanser.Cleanser
import com.ignitrplus.data.pipeline.cleanser.Cleanser.removeDuplicate
import com.ignitrplus.data.pipeline.constants.DqCheckConstants.COLUMNS_CHECK_NULL_CLICKSTREAM
import com.ignitrplus.data.pipeline.dqcheck.DqCheck
import com.ignitrplus.data.pipeline.service.FileWriterService.sqlWrite
import com.ignitrplus.data.pipeline.transform.Transform
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
    val dfNoDuplicateClickStream = Cleanser.removeDuplicate(dfNotNullColClickStream,COLUMNS_PRIMARY_KEY_CLICKSTREAM,Some(EVENT_TIMESTAMP_OPTION))
    val dfNoDuplicateItemDf = removeDuplicate(dfNotNullColItem,COLUMNS_PRIMARY_KEY_ITEM,None)

    /**convert to lowercase */
    val dfLowerCaseClickStream = Cleanser.convertToLowerCase(dfNoDuplicateClickStream,COLUMNS_LOWERCASE_CLICKSTREAM)
    val dfLowerCaseItem = Cleanser.convertToLowerCase(dfNoDuplicateItemDf,COLUMNS_LOWERCASE_ITEM)



    /**DQCheck functions*/
      /**check null values*/
//    val dfCheckNull = DqCheck.checkNull( dfLowerCaseClickStream,COLUMNS_CHECK_NULL_CLICKSTREAM)
//
//    /**join by item id*/
    val joinedData = Transform.join(dfLowerCaseClickStream,dfLowerCaseItem)
    sqlWrite(joinedData,TABLE_NAME,SQL_URL_STAGING)



  }
}
