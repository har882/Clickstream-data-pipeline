package com.ignitrplus.data.pipeline.service

import org.apache.spark.sql.DataFrame

object FileWriterService {

  def writeNullRowsFile(df:DataFrame,writeFormat: String): Unit = {
    df.write
      .option("header",true)
      .format(writeFormat)
      .save("data/output/pipeline-failures/null.csv")



  }

}
