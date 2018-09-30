package com.spsoft.spark.utils

import java.io.IOException
import java.util.Properties

import org.slf4j.LoggerFactory

abstract class PropertiesLoader {

  private val LOG = LoggerFactory.getLogger(classOf[PropertiesLoader])

  val properties: Properties = {
    try {
      val props = new Properties()
      // 获取KAFKA的配置文件
      val pathStream = getClass.getResourceAsStream(inputStream)
      // 倒入配置文件
      props.load(pathStream)
      props
    } catch {
      case error: IOException =>
        LOG.error(s"Cannot read ${inputStream}", error)
        throw error
    }
  }

  def get(propertyName: String): String = {
    properties.getProperty(propertyName)
  }

  def inputStream: String

}
