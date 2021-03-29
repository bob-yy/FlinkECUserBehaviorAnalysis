package com.hypers.data.monitor

import scala.io.{BufferedSource, Source}

// 不支持并发环境下运行
class FileSource(file:String) {

  val fileStream: BufferedSource = Source.fromFile(file)

  def next()




}

object FileSource{
  def main(args: Array[String]): Unit = {
    val path = "C:\\Users\\hypers\\IdeaProjects\\FlinkECUserBehaviorAnalysis\\src\\main\\resources\\UserBehavior.csv"
    val file = new FileSource(path)

    println(file.file)
  }
}
