package com.hypers.data.monitor

import org.slf4j.{Logger, LoggerFactory}

import scala.io.{BufferedSource, Source}

class TimeFileSource(file: String) extends AutoCloseable{
  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  private val fileSource = Source.fromFile(file)
  private val stream: Iterator[String] = fileSource.getLines()

  def nextSecond(second: Int): Array[String] = {
    if(stream.hasNext){
      val str = stream.next()

    }
    stream.take(1).
    strings
  }

  def close(): Unit = {
    try {
      fileSource.close()
    } catch {
      case e: Exception =>
        log.error(s"$this.getClass.getName 关闭失败")
        e.printStackTrace()
    }

  }

}
