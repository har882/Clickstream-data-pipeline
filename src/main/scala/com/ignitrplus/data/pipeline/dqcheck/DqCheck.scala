package com.ignitrplus.data.pipeline.dqcheck

import com.ignitrplus.data.pipeline.constants.ApplicationConstants.ITEM_ID
import com.ignitrplus.data.pipeline.exception.ExceptionFile.{NullValuesException, UnMatchedItemIdException}
import org.apache.spark.sql.DataFrame

object DqCheck {

  def checkNull(df: DataFrame, checkNullColumns: Seq[String]): Unit = {
    var nullDf: DataFrame = df

    for (i <- checkNullColumns) {
      nullDf = df.filter(df(i).isNull)
    }
    if (nullDf.count() > 0) {
      throw new NullValuesException("Null Values found")
    }
  }








}
