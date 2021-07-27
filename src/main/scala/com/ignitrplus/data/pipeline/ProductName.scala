package com.ignitrplus.data.pipeline

import com.ignitrplus.data.pipeline.service.FileReadearService.readFile
import com.ignitrplus.data.pipeline.transform.Tranformation.{dataType, lowerCase}
import com.ignitrplus.data.pipeline.util.ApplicationUtil.createSparkSession

object ProductName {
  def main(args: Array[String]): Unit = {
    dataType
    lowerCase

  }

}
