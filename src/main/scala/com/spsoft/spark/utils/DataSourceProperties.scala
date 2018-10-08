package com.spsoft.spark.utils

import com.spsoft.spark.common.ProjectConstants._
import org.slf4j.LoggerFactory

object DataSourceProperties extends PropertiesLoader {

  private val LOG = LoggerFactory.getLogger(CloseableMysqlUtils.getClass)

  override def inputStream: String = DATABASE_PROPERTIES_PATH


  def main(args: Array[String]): Unit = {
    println(get("url"))
    println(get("username"))
    println(get("password"))
  }
}
