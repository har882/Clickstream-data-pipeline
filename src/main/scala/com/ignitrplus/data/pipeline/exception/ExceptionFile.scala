package com.ignitrplus.data.pipeline.exception

object ExceptionFile {
  class InvalidInputFileException(s:String) extends Exception(s){}

  class NullValuesException(s:String) extends Exception(s){}

  class UnMatchedItemIdException(s:String) extends Exception(s){}
}
