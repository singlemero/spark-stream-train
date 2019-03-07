package com.spsoft.spark.utils

import java.time.Duration

import com.spsoft.spark.utils.EnhanceUtils._
import io.lettuce.core.RedisURI
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.cluster.{ClusterClientOptions, ClusterTopologyRefreshOptions, RedisClusterClient}
import org.slf4j.LoggerFactory

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

  lazy val clusterClient: RedisClusterClient = {
    val topologyRefreshOptions = ClusterTopologyRefreshOptions.builder.enablePeriodicRefresh(Duration.ofMinutes(10)).enableAllAdaptiveRefreshTriggers.build
    val client = RedisClusterClient.create(RedisURIS)
    client.setOptions(ClusterClientOptions.builder.autoReconnect(true).pingBeforeActivateConnection(true)
        .topologyRefreshOptions(topologyRefreshOptions).build)
    client
  }

  private def getConnect() = {
//    val topologyRefreshOptions = ClusterTopologyRefreshOptions.builder.enablePeriodicRefresh(Duration.ofMinutes(10)).enableAllAdaptiveRefreshTriggers.build
//    client
//      .setOptions(ClusterClientOptions.builder.autoReconnect(true).pingBeforeActivateConnection(true)
//      .topologyRefreshOptions(topologyRefreshOptions).build)
    clusterClient.connect
  }



  def foo(str: String)(fun:(String) =>Int)(fun1:(Int)=> String ) = {

    print(fun1(fun(str)))
  }

  def main(args: Array[String]): Unit = {

//    foo("iiiiiii"){f=>f.length}{a:Int=> (a*10).toString}
//    println(get("foo"))
    execute({con=> con.sync().hset("db1","111","0")})
    val db = execute({con=> con.sync().hget("db1","111")})
    println(db)
    val c = execute({con=> con.sync().get("foo")})

    println(c)
    val l = List("1"->List(1,2), "2"-> List(2,3), "3"->List(3,4))
      .foreach(f=> print(f._2))
    clusterClient.shutdown()
  }



  def get(key:String) = {
    autoClose(getConnect()){con =>
      con.sync().get(key)
    }
  }

  def execute[B](fun: (StatefulRedisClusterConnection[String, String]) => B): B ={
    autoClose(clusterClient.connect){ con =>fun(con)}
  }




  def set(key:String, value:String) = {
    autoClose(getConnect()){con =>
      con.sync().set(key, value)
    }
  }


  def hset(key: String, field: String, value: String) = {
//    autoClose(getConnect()){con=> con.sync().hset(key, field, value)}
//    execute({con=> con.sync().hset(key, field, value)})
  }



}
