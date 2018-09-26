package exercise

import java.util
import java.util.{List, Random}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{Cluster, PartitionInfo}

class SimplePartitioner extends  DefaultPartitioner{

  override def partition(topic: String, key: scala.Any, keyBytes: Array[Byte], value: scala.Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    val partitions = cluster.partitionsForTopic(topic)
    val numPartitions = partitions.size
    val availablePartitions = cluster.availablePartitionsForTopic(topic)
    val availableNumPartitions = availablePartitions.size()
    if (keyBytes == null) {
      return super.partition(topic, key, keyBytes, value, valueBytes, cluster)
    }
    else { // hash the keyBytes to choose a partition
      return key match {
        case k: Long => if (availableNumPartitions > 0) Math.round(k % availableNumPartitions) else Math.round(k % numPartitions)
        case k: Int => if (availableNumPartitions > 0) Math.round(k % availableNumPartitions) else Math.round(k % numPartitions)
        case _ => if (availableNumPartitions > 0) Utils.murmur2(keyBytes) % availableNumPartitions else Utils.murmur2(keyBytes) % numPartitions
      }
      //return keyBytes.hashCode() % numPartitions
    }
  }

}
