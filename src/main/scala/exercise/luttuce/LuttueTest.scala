package exercise.luttuce
import io.lettuce.core.{RedisClient, RedisURI}
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands
import java.util

import scala.collection.JavaConverters._

object LuttueTest {

  def main(args: Array[String]): Unit = {

    val redisUri = RedisURI.Builder.redis("192.168.55.226")
      .withPassword("redis")
      .withPort(6380)
      .build();
//    val client = RedisClient.create(redisUri)
    val node1 = RedisURI.Builder.redis("192.168.55.209").withPassword("redis").withPort(6381).build()//RedisURI.create("192.168.55.209", 6381)
    val node2 = RedisURI.Builder.redis("192.168.55.209").withPassword("redis").withPort(6382).build()//RedisURI.create("192.168.55.209", 6382)
    val node3 = RedisURI.Builder.redis("192.168.55.209").withPassword("redis").withPort(6383).build()//RedisURI.create("192.168.55.209", 6383)
    val nodeList = List(node1, node2, node3)
    val clusterClient = RedisClusterClient.create(nodeList.asJava)
    val connection = clusterClient.connect
//    val connection = client.connect
    val syncCommands = connection.sync

    syncCommands.set("foo","this is scala luttuce test")

    val value = syncCommands.get("foo")
    print(value)
    connection.close
    clusterClient.shutdown()
//    client.shutdown()
  }
}
