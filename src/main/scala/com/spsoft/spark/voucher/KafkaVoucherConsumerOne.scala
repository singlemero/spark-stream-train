package com.spsoft.spark.voucher


import com.spsoft.spark.utils.{ApplicationProperties, KafkaProperties}
import com.spsoft.spark.voucher.serializer.{DateToLongSerializer, VoucherDeserializer}
import com.spsoft.spark.voucher.vo._
import org.apache.commons.lang.math.RandomUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


/**
  *
  */
object KafkaVoucherConsumerOne {

  val producer = new KafkaProducer[String, String](KafkaProperties.get())

  val GROUP_NAME = "voucherGroupOne11"

  private val LOG = LoggerFactory.getLogger(KafkaVoucherConsumerOne.getClass)

  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "/app/soft/hadoop")

    val sparkSession = SparkSession.builder()
      .appName("ClassifyVoucherCode")
//      .master(ApplicationProperties.get("spark.master"))
      .config("spark.streaming.stopGracefullyOnShutdown","true")
      .config("spark.sql.session.timeZone","Asia/Shanghai")
      .getOrCreate()

    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(1))
    val params = KafkaProperties.get(Map("value.deserializer" -> classOf[VoucherDeserializer], "group.id" -> GROUP_NAME ))
    println(params)
    //var (receiveTopic, sendTopic) = (Array("TopicOne"), "TopicTwo")
    var (receiveTopic, sendTopic) = (Array("Voucher4Spark"), "VoucherItems4Spark")
    val stream = KafkaUtils.createDirectStream[String, Voucher](
      streamingContext,
      PreferConsistent,
      Subscribe[String, Voucher](receiveTopic, params.asScala)
    )

    val check = false

    stream.foreachRDD(rdd=>{
//      LOG.warn(s"${rdd}")
      //println(rdd)
      //val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

        rdd.foreachPartition(f => {
          f.foreach(println)
        })
        if (!rdd.isEmpty()) {
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd.foreachPartition(partitionOfRecords => {
            val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)

            implicit val formats = DefaultFormats + new DateToLongSerializer

            partitionOfRecords.flatMap(v => {
              //查找含本年利润的凭证详细
              val profits = v.value().items.find(p => "本年利润".equals(p.subjectFullName))
              //分解凭证细表
              val emptyNum = BigDecimal(0)
              import com.spsoft.spark.hint.IntHints._
              v.value().items.map(i => {
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

//                val monthPeriod = (v.value().accountPeriod / 100).untilYm()
//                val r = RandomUtils.nextInt(monthPeriod.length)
                //              SubjectBalanceSlim(v.value().companyId, v.value().accountPeriod/100 - 100 , i.subjectCode.replace("1101","1231"), debit, debitQty, credit, creditQty, debitPure, creditPure)
                //SubjectBalanceSlim(v.value().companyId, v.value().accountPeriod/100 - 100 , i.subjectCode, debit, debitQty, credit, creditQty, debitPure, creditPure)
                //SubjectBalanceSlim(v.value().companyId, monthPeriod(r) , i.subjectCode, -1*debit, debitQty, -1*credit, creditQty, debitPure, creditPure)
                SubjectBalanceSlim(v.value().companyId, v.value().accountPeriod, i.subjectCode, debit, debitQty, credit, creditQty, debitPure, creditPure)
              })
            }).flatMap(m => {
              //分解凭证细表的科目层级,如100020003000分解成100020003000,10002000,1000三条明细记录
              //var l = List[SubjectBalanceSlim]()
              for (a <- 1 to m.subjectCode.length if a % 4 == 0) yield {
                //l = l :+ SubjectBalanceSlim(m.companyId, m.accountPeriod, m.subjectCode.take(a), m.currentDebitAmount, m.currentDebitQty, m.currentCreditAmount, m.currentCreditQty, m.currentDebitNocarryAmount, m.currentCreditNocarryAmount)
                SubjectBalanceSlim(m.companyId, m.accountPeriod, m.subjectCode.take(a), m.currentDebitAmount, m.currentDebitQty, m.currentCreditAmount, m.currentCreditQty, m.currentDebitNocarryAmount, m.currentCreditNocarryAmount)
              }
              //l
              //发送到kafka做下一步处理
            }).foreach(r => {
              //此处无法保证事务唯一性
//              println(r.toString)
              LOG.info(s"${TaskContext.get.partitionId} ${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset} ${r.toString}")
              //利用这里分区，但这里分区是对kafka 的分区
              producer.send(new ProducerRecord[String, String](sendTopic, s"${r.companyId}${r.subjectCode}", write(r)))
            })
          })
          //保存offset
          stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        }



    })

    //人工中断
    sys.ShutdownHookThread {
      println("Gracefully stopping Spark Streaming Application at "+ new java.util.Date())
      streamingContext.stop(true, true)
      println("Application stopped at "+ new java.util.Date())
    }


    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop(true,  true);

  }

}
