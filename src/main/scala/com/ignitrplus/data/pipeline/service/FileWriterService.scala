package com.ignitrplus.data.pipeline.service

import org.apache.spark.sql.{DataFrame, SparkSession}

object FileWriterService {

  def writeNullRowsFile(df:DataFrame): Unit = {
    df.write
      .option("header",true)
      .format("csv")
      .save("data/output/pipeline-failures/null.csv")



  }

}
