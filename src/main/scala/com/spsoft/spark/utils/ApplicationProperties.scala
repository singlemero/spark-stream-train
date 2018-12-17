package com.spsoft.spark.utils

import com.spsoft.spark.common.ProjectConstants
import io.lettuce.core.RedisURI
import scala.collection.JavaConverters._
object ApplicationProperties extends PropertiesLoader{
  override def inputStream: String = ProjectConstants.APPLICATION_PATH

  def main(args: Array[String]): Unit = {
    val nodes = get("redis.nodes").split(",")
    val pwd = get("redis.password")
    val l = nodes.map(p =>{
      val hostAndPort = p.split(":")
      RedisURI.Builder.redis(hostAndPort(0)).withPassword(pwd).withPort(hostAndPort(1).toInt).build()
    }).toList.asJava

    print(l)

  }
}
