package com.ignitrplus.data.pipeline.dqcheck

import com.ignitrplus.data.pipeline.constants.ApplicationConstants.ITEM_ID
import com.ignitrplus.data.pipeline.exception.ExceptionFile.{NullValuesException, UnMatchedItemIdException}
import org.apache.spark.sql.DataFrame

object DqCheck {

  def checkNull(df: DataFrame, checkNullColumns: Seq[String]):Unit = {
    var nullDf: DataFrame = df

    for (i <- checkNullColumns) {
      nullDf = df.filter(df(i).isNull)
    }
    if (nullDf.count() > 0) {
      throw new NullValuesException("Null Values found")
    }
  }
  /** item id which is not present in item dataset but present in clickstream dataset */
    /**for checking it before joining dataset */
  def checkUnMatchedItemId(dfClickstream: DataFrame,dfItem: DataFrame):Unit={
    val dfUnMatchedItemId = dfClickstream.join(dfClickstream,dfClickstream(ITEM_ID) ===  dfItem(ITEM_ID),"leftanti")
    dfUnMatchedItemId.show()
    if(dfUnMatchedItemId.count()>0){
      throw new UnMatchedItemIdException("There is an unmatched item_id in clickstream dataset")
    }
  }


}
