package exercise

import com.spsoft.spark.voucher.MyPartitioner
import com.spsoft.spark.voucher.vo.{DateToLongSerializer, SubjectBalanceSlim}
import exercise.serialization.PersonDeserializer
import exercise.sql.Person
import kafka.message.MessageAndMetadata
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats

import scala.collection.mutable
import scala.collection.JavaConverters._
import org.json4s.jackson.Serialization.{write, read => sread}


/**
  * 应用程序管理offset
  */
object KafkaWordCountOffsetConsumer {

  //    val ZK_NODES = "192.168.55.235:9092,192.168.55.236:9092"
  val ZK_NODES = "192.168.55.226:9092"

  val client = {
    val client = CuratorFrameworkFactory
      .builder
      .connectString("192.168.55.226:2181")
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .namespace("klhNs")
      .build()
    client.start()
    client
  }

  val Globe_kafkaOffsetPath = "/spark/test/offsets"

  // 路径确认函数  确认ZK中路径存在，不存在则创建该路径
  def ensureZKPathExists(path: String)={

    if (client.checkExists().forPath(path) == null) {
      client.create().creatingParentsIfNeeded().forPath(path)
    }

  }

  def storeOffsets(offsetRange: Array[OffsetRange], groupName:String) = {

    for (o <- offsetRange){
      val zkPath = s"${Globe_kafkaOffsetPath}/${groupName}/${o.topic}/${o.partition}"
      ensureZKPathExists(zkPath)
      // 向对应分区第一次写入或者更新Offset 信息
      //println("---Offset写入ZK------\nTopic：" + o.topic +", Partition:" + o.partition + ", Offset:" + o.untilOffset)
      client.setData().forPath(zkPath, o.untilOffset.toString.getBytes())
    }
  }

  def getFromOffset(topics: Array[String], groupName:String):Map[TopicPartition, Long] = {

    // Kafka 0.8和0.10的版本差别，0.10 为 TopicPartition   0.8 TopicAndPartition
    var fromOffset: Map[TopicPartition, Long] = Map()


    val m = topics.flatMap(t=>{
      val zkTopicPath = s"${Globe_kafkaOffsetPath}/${groupName}/${t}"
      // 检查路径是否存在
      ensureZKPathExists(zkTopicPath)
      val childrens = client.getChildren().forPath(zkTopicPath).asScala
      val offSets: mutable.Buffer[(TopicPartition, Long)] = for {
        p <- childrens
      }
        yield {

          // 遍历读取子节点中的数据：即 offset
          val offsetData = client.getData().forPath(s"$zkTopicPath/$p")
          // 将offset转为Long
          val offSet = java.lang.Long.valueOf(new String(offsetData)).toLong
          // 返回  (TopicPartition, Long)
          (new TopicPartition(t, Integer.parseInt(p)), offSet)
        }
      offSets
    }).toMap
   print(m)
    m
  }


  /**
    * earliest 当分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费。
    * latest 当分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据。
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val groupName = "thisGroup"
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[4]");
    val streamingContext = new StreamingContext(sparkConf, Seconds(1))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> ZK_NODES,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[PersonDeserializer],
      //"value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupName
      //"auto.offset.reset" -> "latest",
      //"enable.auto.commit" -> (true: java.lang.Boolean)
    )

    //import streamingContext.implicits._




    val topics = Array("TopicC")
    val fromOffset = getFromOffset(topics, groupName)
    val stream = KafkaUtils.createDirectStream[String, Person](
      streamingContext,
      PreferConsistent,
      Subscribe[String, Person](topics, kafkaParams, fromOffset)
    )


    //stream.foreachRDD(dd => dd.name, dd.)
    //stream.foreachRDD(dd => dd.name, dd.)

    stream//.map(m=> {
  /*
      //new ConsumerRecord(m.topic(), m.partition(), m.offset(),m.timestamp(), m.timestampType(), m.checksum(), m.serializedKeySize(),m.serializedValueSize(), m.key(), m.value())
    //})

        .map(m=> {
      //implicit val formats = DefaultFormats + new DateToLongSerializer
      //val p = sread[Person](m.value())
      (m.value().id, m)
      //m
    }).groupByKey(new MyPartitioner(5))

*/
    .foreachRDD(rdd =>
      {

        if(!rdd.isEmpty()){
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd.foreachPartition {
            partitionOfRecords => {
              val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
              //println("p "+partitionOfRecords)
              if(o.fromOffset != o.untilOffset){
                //println(s"${TaskContext.get.partitionId} ${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
                partitionOfRecords.foreach( record => println(record))
              }
              //需记录错误offset
            }
          }
          //程序管理offset
          //storeOffsets(offsetRanges, groupName)
          //kafka 管理offset
          stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
          /**
          rdd.map(m=> {
            implicit val formats = DefaultFormats + new DateToLongSerializer
            val p = sread[Person](m.value())
            (s"${p.id}${p.name}", m)
          }).groupByKey(new MyPartitioner(3))
            .foreachPartition(partitionOfRecords => {
              println("p "+partitionOfRecords)

              partitionOfRecords.foreach( record => print(record))
            })
            */
          println("offsetRanges:"+ offsetRanges)


        }
      })



    /**
    stream.foreachRDD(rdd=>{
      rdd.foreachPartition(partitionOfRecords=> {
        partitionOfRecords.foreach( record => {
          print("pp: ")
          println(record)
        })
      })
    })
      */
    //stream.map(record => (record.key, record.value)).foreachRDD(dd => dd.)
    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop(true, true);

  }
}
