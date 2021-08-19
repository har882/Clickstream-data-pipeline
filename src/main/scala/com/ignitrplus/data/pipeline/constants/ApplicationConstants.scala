package com.ignitrplus.data.pipeline.constants


object ApplicationConstants {

  //SPARK_SESSION
  val MASTER: String = "local"
  val APP_NAME: String = "Clickstream Pipeline"

  //  val SPARK_CONF_FILE_NAME = "spark.conf"
  //  val SPARK_CONF = getSparkConf(SPARK_CONF_FILE_NAME)
  //  implicit val spark = createSparkSession(SPARK_CONF)

  //DATASET
  val CLICKSTREAM_DATASET: String = "data/input02/clickstream/clickstream_log.csv"
  val ITEM_DATASET: String = "data/input02/item/item_data.csv"

  //null values writing path
  val CLICKSTREAM_NULL_ROWS_DATASET_PATH: String ="data/output/pipeline-failures/clickstream_null_values"
  val ITEM_NULL_ROWS_DATASET_PATH: String ="data/output/pipeline-failures/item_null_values"

  //output dataframe path  /**Temporary*/
  val CLICKSTREAM_FILE_READER_DATASET:String ="data/output/output-dataframe-of-each-function/fileReader/clickstream"
  val ITEM_FILE_READER_DATASET:String ="data/output/output-dataframe-of-each-function/fileReader/item"

  val CLICKSTREAM_DATA_TYPE_DATASET: String ="data/output/output-dataframe-of-each-function/dataTypeValidation/clickstream"
  val ITEM_DATA_TYPE_DATASET: String ="data/output/output-dataframe-of-each-function/dataTypeValidation/item"

  val CLICKSTREAM_TRIM_DATASET: String ="data/output/output-dataframe-of-each-function/trimColumn/clickstream"
  val ITEM_TRIM_DATASET: String ="data/output/output-dataframe-of-each-function/trimColumn/item"

  val CLICKSTREAM_NOT_NULL_DATASET: String ="data/output/output-dataframe-of-each-function/filterNotNullRow/clickstream"
  val ITEM_NOT_NULL_DATASET: String ="data/output/output-dataframe-of-each-function/filterNotNullRow/item"

  val CLICKSTREAM_DEDUPLICATE_DATASET: String ="data/output/output-dataframe-of-each-function/removeDuplicates/clickstream"
  val ITEM_DEDUPLICATE_DATASET: String ="data/output/output-dataframe-of-each-function/removeDuplicates/item"

  val CLICKSTREAM_LOWERCASE_DATASET: String ="data/output/output-dataframe-of-each-function/convertToLowerCase/clickstream"
  val ITEM_LOWERCASE_DATASET: String ="data/output/output-dataframe-of-each-function/convertToLowerCase/item"

  //DATASET FORMAT
  val READ_FORMAT:String = "csv"
  val WRITE_FORMAT:String = "csv"

  // column name Clickstream
  val EVENT_TIMESTAMP: String = "event_timestamp"
  val SESSION_ID: String = "session_id"
  val ITEM_ID: String = "item_id"
  val REDIRECTION_SOURCE: String = "redirection_source"
  val VISITOR_ID: String = "visitor_id"

  // column name Item
  val DEPARTMENT_NAME: String = "department_name"
  val ITEM_PRICE: String = "item_price"


  //column for Changing DATATYPE
  val COLUMNS_VALID_DATATYPE_CLICKSTREAM: Seq[String] = Seq(ApplicationConstants.EVENT_TIMESTAMP)
  val COLUMNS_VALID_DATATYPE_ITEM: Seq[String] = Seq(ApplicationConstants.ITEM_PRICE)

  //new DATATYPE
  val NEW_DATATYPE_CLICKSTREAM:Seq[String]= Seq("timestamp")
  val NEW_DATATYPE_ITEM:Seq[String]= Seq("float")

  //Primary key
  val COLUMNS_PRIMARY_KEY_CLICKSTREAM: Seq[String] = Seq(ApplicationConstants.SESSION_ID, ApplicationConstants.ITEM_ID)
  val COLUMNS_PRIMARY_KEY_ITEM: Seq[String] = Seq(ApplicationConstants.ITEM_ID)

  //Lowercase column
  val COLUMNS_LOWERCASE_CLICKSTREAM: Seq[String] = Seq(ApplicationConstants.REDIRECTION_SOURCE)
  val COLUMNS_LOWERCASE_ITEM: Seq[String] = Seq(ApplicationConstants.DEPARTMENT_NAME)

  val FAILURE_EXIT_CODE:Int = 1

  val ROW_NUMBER:String = "row_number"
  val EVENT_TIMESTAMP_OPTION:String= "event_timestamp"
}
