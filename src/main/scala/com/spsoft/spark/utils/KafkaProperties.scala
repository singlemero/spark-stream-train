package com.spsoft.spark.utils

import java.util

import com.spsoft.spark.voucher.common.ProjectConstants
import org.slf4j.LoggerFactory


object KafkaProperties extends PropertiesLoader {

  private val LOG = LoggerFactory.getLogger(CloseableMysqlUtils.getClass)

  override def inputStream: String = ProjectConstants.KAFKA_PROPERTIES_PATH

  def get(map: Map[String, Object] = Map[String, Object]()):util.Map[String, Object] = {
    val m = new util.HashMap[String, Object]()
    val elem = properties.propertyNames()
    while (elem.hasMoreElements){
      val c = elem.nextElement()
      m.put(c.toString, properties.getProperty(c.toString))
    }
    map.foreach(f=> m.put(f._1, f._2))
    m
  }

}
