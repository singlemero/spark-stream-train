package com.spsoft.spark.voucher

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.spsoft.spark.voucher.KafkaVoucherConsumerOne.{ZK_NODES, kafkaProducer, listenTopic}
import com.spsoft.spark.voucher.serializer.VoucherDeserializer
import com.spsoft.spark.voucher.vo._
import org.apache.commons.lang.math.RandomUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write, read => sread}

import scala.collection.JavaConverters._


/**
  *
  */
object KafkaVoucherConsumerOne1 {

      val ZK_NODES = "192.168.55.235:9092,192.168.55.236:9092"
  //val ZK_NODES = "192.168.55.226:9092"

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> ZK_NODES,
    "key.deserializer" -> classOf[LongDeserializer],
    "value.deserializer" -> classOf[VoucherDeserializer],
    "group.id" -> "voucherGroupTwo",
    //"auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  def main(args: Array[String]): Unit = {



    val sparkConf = new SparkConf().setAppName("ttt").setMaster("local[4]");
    val streamingContext = new StreamingContext(sparkConf, Seconds(1))
    //import streamingContext.implicits._
    val topics = Array("TopicOne")
    val stream = KafkaUtils.createDirectStream[String, Voucher](
      streamingContext,
      PreferConsistent,
      Subscribe[String, Voucher](topics, kafkaParams)
    )
    stream.foreachRDD(rdd=>{
      //println(rdd)
      if(!rdd.isEmpty()){
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition(partitionOfRecords=>{
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          //partitionOfRecords.foreach(println)
          //stream.asInstanceOf[CanCommitOffsets].commitAsync(Array(o))



          implicit val formats = DefaultFormats + new DateToLongSerializer
          /**partitionOfRecords.map(record=> {
          println(record)
          transformJson(record.value())
        })*/
          partitionOfRecords.flatMap(v=>{
            //查找含本年利润的凭证详细
            val profits = v.value().items.find(p=>"本年利润".equals(p.subjectFullName))

            //定义scala的List
            //var l = List[SubjectBalanceSlim]()
            //分解凭证细表
            val emptyNum = BigDecimal(0)
            //                               vv.isEmpty
            import IntCover._
            v.value().items.map(i=>{
              //借方金额、数量
              val debit = if (i.lendingDirection == 1) i.subjectAmount else emptyNum
              val debitQty = if (i.lendingDirection == 1) i.qty else emptyNum
              //借方不含结转金额
              val debitPure = if (profits.isEmpty) debit else emptyNum
              //贷方金额、数量
              val credit = if (i.lendingDirection == 2) i.subjectAmount else emptyNum
              val creditQty = if (i.lendingDirection == 2) i.qty else emptyNum
              //贷方不含结转金额
              val creditPure = if (profits.isEmpty) credit else emptyNum
              //设置正确的属期

              //import DateCover._

              val monthPeriod = (v.value().accountPeriod/100).months()
              val r = RandomUtils.nextInt(monthPeriod.length)
//              SubjectBalanceSlim(v.value().companyId, v.value().accountPeriod/100 - 100 , i.subjectCode.replace("1101","1231"), debit, debitQty, credit, creditQty, debitPure, creditPure)
              //SubjectBalanceSlim(v.value().companyId, v.value().accountPeriod/100 - 100 , i.subjectCode, debit, debitQty, credit, creditQty, debitPure, creditPure)
              SubjectBalanceSlim(v.value().companyId, monthPeriod(r) , i.subjectCode, -1*debit, debitQty, -1*credit, creditQty, debitPure, creditPure)
              //SubjectBalanceSlim(v.value().companyId, v.value().accountPeriod/100,  i.subjectCode, debit, debitQty, credit, creditQty, debitPure, creditPure)
            })
          }).flatMap(m=> {
            //分解凭证细表的科目层级,如100020003000分解成100020003000,10002000,1000三条明细记录
            //var l = List[SubjectBalanceSlim]()
            for (a <- 1 to m.subjectCode.length if a % 4 == 0) yield {
              //l = l :+ SubjectBalanceSlim(m.companyId, m.accountPeriod, m.subjectCode.take(a), m.currentDebitAmount, m.currentDebitQty, m.currentCreditAmount, m.currentCreditQty, m.currentDebitNocarryAmount, m.currentCreditNocarryAmount)
              SubjectBalanceSlim(m.companyId, m.accountPeriod, m.subjectCode.take(a), m.currentDebitAmount, m.currentDebitQty, m.currentCreditAmount, m.currentCreditQty, m.currentDebitNocarryAmount, m.currentCreditNocarryAmount)
            }
            //l
            //发送到kafka做下一步处理
          }).foreach(r => {//此处无法保证事务唯一性
            //println(r.toString)
            println(s"${TaskContext.get.partitionId} ${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset} ${r.toString}")
            //利用这里分区，但这里分区是对kafka 的分区
            producer.send(new ProducerRecord[String, String]("TopicTwo", s"${r.companyId}${r.subjectCode}",write(r)))
          })
        })
        //保存offset
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })
    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop(true,  true);

  }


  def producer = {
    if(kafkaProducer == null){
      val props = new Properties()
      props.put("metadata.broker.list", ZK_NODES)
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("bootstrap.servers", ZK_NODES)
      //    props.put("partitioner.class", "com.fortysevendeg.biglog.SimplePartitioner")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("producer.type", "async")
      props.put("request.required.acks", "1")

      kafkaProducer = new KafkaProducer[String, String](props)
    }
    kafkaProducer
  }



}
