package com.hypers.data.monitor

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * 从指定文件中按照时间间隔读取相应数据。支持一次读取一秒的数据。
 * <p>
 * 要求：文件中全部数据的格式统一，包含时间字段。并在构建对象时传入相关参数。
 * <p>
 * 这个类是线程安全的。
 * <p>
 *
 * @param file      文件名
 * @param rowNumber 时间字段在数据行中的位置，从零开始计算
 * @param separator 行字段的分隔符，只支持一层分隔
 */
class TimeFileSource(file: String, rowNumber: Int, separator: String) extends AutoCloseable {
  private val log: Logger = LoggerFactory.getLogger(this.getClass)
  private val fileSource = Source.fromFile(file)
  private val stream: Iterator[String] = fileSource.getLines()
  private val logParse: LogParse = LogParse(rowNumber, separator)
  private var nextData: String = if (stream.hasNext) stream.next() else null
  private val buffer = ArrayBuffer[String]()

  /**
   * 获取数据源一秒内的所有数据。
   * <p>
   * 返回的数组是不可变的。
   * <p>
   * 如果未进行hasNext判断，有可能会抛出异常。
   *
   * @return Array[String],
   */
  def nextSecond(): Array[String] = {
    require(! hasNext, "文件为空！")
    buffer.clear()
    buffer += nextData
    val dataTime = logParse.parse(nextData)
    nextData = if (stream.hasNext) stream.next() else null

    while (nextData != null && logParse.parse(dataTime) == logParse.parse(nextData)) {
      buffer += nextData
      nextData = if (stream.hasNext) stream.next() else null
    }
    buffer.toArray
  }

  /**
   * 判断该数据源是否还有数据。
   * <p>
   * 因为保存了文件流最开始的一个元素，所以TimeFileSource为空和文件流为空不一致。
   * 这里是判断TimeFileSource是否为null。
   * <p>
   *
   * @return Boolean
   */
  def hasNext: Boolean = if (nextData == null) false else true

  /**
   * 关闭文件源
   * <p>
   * 这里只考虑打印出错信息，抛出异常让上层调用捕获并做出相关处理。
   */
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

object TimeFileSource {
  def apply(file: String, rowNumber: Int, separator: String): TimeFileSource =
    new TimeFileSource(file, rowNumber, separator)

  def main(args: Array[String]): Unit = {
    "G:\\JAVA\\FlinkECUserBehaviorAnalysis\\src\\main\\resources\\UserBehavior.csv"
    val ubPath = "src/main/resources/UserBehavior.csv"
    val rowNumber = 4
    val separator = ","
    var day = 60 * 60 * 12
    var count = 1
    val source = TimeFileSource(ubPath, rowNumber, separator)

    while (day > 0) {
      println(s"第 $count 秒:")
      source.nextSecond().foreach(println)
      Thread.sleep(1000)
      count += 1
      day -= 1
    }

    //    println("第1秒:")
    //    source.nextSecond().foreach(println)
    //    Thread.sleep(1000)
    //
    //    println("第2秒:")
    //    source.nextSecond().foreach(println)
    //    Thread.sleep(1000)
    //
    //    println("第3秒:")
    //    source.nextSecond().foreach(println)

    source.close()

  }
}
