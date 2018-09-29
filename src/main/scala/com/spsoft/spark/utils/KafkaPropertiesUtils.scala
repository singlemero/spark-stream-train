package com.spsoft.spark.utils

import java.io.IOException
import java.util
import java.util.Properties

import com.spsoft.spark.voucher.common.ProjectConstants._
import org.slf4j.LoggerFactory


object KafkaPropertiesUtils {

  private val LOG = LoggerFactory.getLogger(MysqlPoolUtils.getClass)

  val properties: Properties = {
    try {
      val props = new Properties()
      // 获取KAFKA的配置文件
      val pathStream = getClass.getResourceAsStream(KAFKA_PROPERTIES_PATH)
      // 倒入配置文件
      props.load(pathStream)
      props
    } catch {
      case error: IOException =>
        LOG.error(s"Cannot read ${KAFKA_PROPERTIES_PATH}", error)
        throw error
    }
  }


  def getProperties(map: Map[String, Object] = Map[String, Object]()):util.Map[String, Object] = {
    //val props = new Properties()
    //properties
    //map.foreach(f=> props.setProperty(f._1, f._2))
    val m = new util.HashMap[String, Object]()
    val elem = properties.propertyNames()
    while (elem.hasMoreElements){
      val c = elem.nextElement()
      m.put(c.toString, properties.getProperty(c.toString))
    }
    map.foreach(f=> m.put(f._1, f._2))
    m
  }

  def main(args: Array[String]): Unit = {
    println(getProperties())
  }
}
