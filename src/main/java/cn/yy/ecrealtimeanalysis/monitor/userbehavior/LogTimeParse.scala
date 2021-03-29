package com.hypers.data.monitor

class LogTimeParse(rowNumber:Int, separator:String = ",") {

  def parse(line:String):String = {
    val row = line.split(separator)
    if(row.length>rowNumber){
      row(rowNumber)

    }
  }

}

object LogTimeParse{

}
