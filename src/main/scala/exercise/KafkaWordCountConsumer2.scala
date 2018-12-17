package exercise

import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConverters._

object KafkaWordCountConsumer2 {

      val ZK_NODES = "192.168.55.235:9092,192.168.55.236:9092"
  //val ZK_NODES = "192.168.55.226:9092"

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[4]");
    val streamingContext = new StreamingContext(sparkConf, Seconds(1))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> ZK_NODES,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "voucherGroupTwo",
      //"auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //import streamingContext.implicits._
    val topics = Array("Voucher4Spark")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    import org.apache.spark.streaming.api.java.JavaPairDStream
    import scala.Tuple2
    //stream.foreachRDD(dd => dd.name, dd.)
    //stream.foreachRDD(dd => dd.name, dd.)



    stream.foreachRDD(rdd =>
      {

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        /**
        for(o <- offsetRanges){
          print("this.offset : ")
          println(o)
        }
          */
        rdd.foreachPartition {
          partitionOfRecords =>
            val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
            //println("p "+partitionOfRecords)
            if(o.fromOffset != o.untilOffset){
              println(s"${TaskContext.get.partitionId} ${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
              partitionOfRecords.foreach( record => println(s"${TaskContext.get.partitionId} ${o.topic} ${o.partition} ${record.toString}"))
            }
            //partitionOfRecords.foreach(println)
        }
        //kafka 自身方式提交offset
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      })


    //stream.map(record => (record.key, record.value)).foreachRDD(dd => dd.)
    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop(true, true);
  }
}
