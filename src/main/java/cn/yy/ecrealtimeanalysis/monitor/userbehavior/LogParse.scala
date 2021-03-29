package com.hypers.data.monitor

import org.slf4j.{Logger, LoggerFactory}

/**
 * 通用的格式解析类。可以根据构建对象是传入的切分参数和指定行，获取传入数据对应的字段。
 *
 * @param rowNumber 需要提取的字段的位置。
 * @param separator 分隔符
 */
class LogParse(rowNumber: Int, separator: String) {
  private val log: Logger = LoggerFactory.getLogger(LogParse.getClass)

  /**
   * 根据字段位置和分隔符解析数据，并返回该字段值。
   * <p>
   * 切分后的字段个数不能<=要求的rowNumber
   *
   * @param line 一行数据
   * @return 指定的字段值
   */
  def parse(line: String): String = {
    require(line != null, "输入数据为null！")

    val row = line.split(separator)
    if (row.length > rowNumber) {
      row(rowNumber)
    } else {
      log.error("解析数据的字段位置小于、等于指定的字段位置！")
      throw new ArrayIndexOutOfBoundsException()
    }
  }

}

object LogParse {
  def apply(rowNumber: Int, separator: String = ","): LogParse = new LogParse(rowNumber, separator)

  def main(args: Array[String]): Unit = {
    val userBehavior1 = "543462,1715,1464116,pv,1511658000"
    val userBehavior2 = "662867,2244074,1575622,pv,1511658000"
    val rowNumber = 4
    val separator = ","

    val parse = LogParse(rowNumber, separator)
    println(parse.parse(userBehavior1))
    println(parse.parse(userBehavior1))

    println(parse.parse(userBehavior2))

  }
}
