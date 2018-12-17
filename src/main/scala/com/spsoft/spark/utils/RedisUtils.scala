package com.spsoft.spark.utils

import com.zaxxer.hikari.HikariConfig
import io.lettuce.core.RedisURI
import io.lettuce.core.cluster.RedisClusterClient
import org.slf4j.LoggerFactory
import java.lang.AutoCloseable
import EnhanceUtils._


import scala.collection.JavaConverters._

object RedisUtils {
  private val LOG = LoggerFactory.getLogger(RedisUtils.getClass)

  lazy val RedisURIS:java.util.List[RedisURI] = {
    val nodes = ApplicationProperties.get("redis.nodes").split(",")
    val pwd = ApplicationProperties.get("redis.password")
    val nodesList = nodes.map(p =>{
      val hostAndPort = p.split(":")
      RedisURI.Builder.redis(hostAndPort(0)).withPort(hostAndPort(1).toInt).withPassword(pwd).build()
    }).toList.asJava
    nodesList
  }


  private def getConnect() = {
    val clusterClient = RedisClusterClient.create(RedisURIS)
    clusterClient.connect
  }



  def foo(str: String)(fun:(String) =>Int)(fun1:(Int)=> String ) = {

    print(fun1(fun(str)))
  }

  def main(args: Array[String]): Unit = {

    foo("iiiiiii"){f=>f.length}{a:Int=> (a*10).toString}
    print(get("foo"))
    val l = List("1"->List(1,2), "2"-> List(2,3), "3"->List(3,4))
      .foreach(f=> print())
  }

  def get(key:String) = {
    autoClose(getConnect()){con =>
      con.sync().get(key)
    }
    /**
    try(val conn = getCon()){

    }
    try{
      val command = getCon().sync()
      command.get(key)
    }finally {

    }
      */

  }

  def set(key:String, value:String) = {



    autoClose(getConnect()){con =>
      con.sync().set(key, value)
    }
  }



}
