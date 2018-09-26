package com.spsoft.spark.voucher

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.spsoft.spark.voucher.vo._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read => sread}

import scala.collection.JavaConverters._


/**
  *
  */
object KafkaVoucherConsumer2 {

  private var streamingContext: StreamingContext = null
  /**
    * kafka监听TOPICß
    * @param topics
    * @return
    */
  def listenTopic(topics: String*): InputDStream[ConsumerRecord[String, String]] = {
    KafkaUtils.createDirectStream[String, String](
      streamContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParmas)
    )
  }

  def streamContext: StreamingContext = {
    if(streamingContext == null){
      streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(1))
    }
    streamingContext
  }

  /**
    * kafka参数
    * @return
    */
  def kafkaParmas: Map[String,Object] = {
    Map[String, Object](
      "bootstrap.servers" -> "192.168.55.235:9092,192.168.55.236:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
  }

  def sparkSession: SparkSession = {
    SparkSession.builder().appName("JdbcVoucherOperation").master("local[4]").getOrCreate()
  }

  def getTableDF(talbe: String): DataFrame = {
    val url = "jdbc:mysql://192.168.55.215:8066/qf_accdb?characterEncoding=utf8"
    val properties = new Properties()
    properties.put("user","qf_user1")
    properties.put("password","hwsofti201710")
    sparkSession.read.jdbc(url, talbe, properties)
  }

  def transformJson(str: String): Voucher = {
    implicit val formats = DefaultFormats + new DateToLongSerializer
    sread[Voucher](str)
  }

  def main(args: Array[String]): Unit = {
    val sc = sparkSession

    listenTopic("TopicOne").map(dstream=> {
      //val offsetRanges = dstream.asInstanceOf[HasOffsetRanges].offsetRanges
      println(dstream)
      transformJson(dstream.value())

    }).cache().flatMap(v=>{
      var l = List[SubjectBalanceSlim]()

      v.items.map(i=>{
        val debit = if (i.lendingDirection == 1) i.subjectAmount else BigDecimal(0)
        val debitQty = if (i.lendingDirection == 1) i.qty else BigDecimal(0)
        //贷金额
        val credit = if (i.lendingDirection == 2) i.subjectAmount else BigDecimal(0)
        val creditQty = if (i.lendingDirection == 2) i.qty else BigDecimal(0)
        SubjectBalanceSlim(v.companyId, v.accountPeriod, i.subjectCode, debit, debitQty, credit, creditQty, 0, 0)
      })
    }).flatMap(m=> {
      var l = List[SubjectBalanceSlim]()
      for (a <- 1 to m.subjectCode.length if a % 4 == 0) {
        l = l :+ SubjectBalanceSlim(m.companyId, m.accountPeriod, m.subjectCode.take(a), m.currentDebitAmount, m.currentDebitQty, m.currentCreditAmount, m.currentCreditQty, m.currentDebitNocarryAmount, m.currentCreditNocarryAmount)
      }
      l
    }).foreachRDD(rdd=> {

      //rdd.partitioner
      //rdd.partitioner = None

      import sc.sqlContext.implicits._

      val c = rdd.toDF().groupBy("companyId","accountPeriod", "subjectCode")
        .sum( "currentDebitAmount" , "currentDebitQty","currentDebitNocarryAmount", "currentCreditAmount", "currentCreditQty","currentCreditNocarryAmount")
        .withColumnRenamed("sum(currentDebitAmount)", "currentDebitAmount")
        .withColumnRenamed("sum(currentDebitQty)", "currentDebitQty")
        .withColumnRenamed("sum(currentDebitNocarryAmount)", "currentDebitNocarryAmount")
        .withColumnRenamed("sum(currentCreditAmount)", "currentCreditAmount")
        .withColumnRenamed("sum(currentCreditQty)", "currentCreditQty")
        .withColumnRenamed("sum(currentCreditNocarryAmount)", "currentCreditNocarryAmount")
      if(c.count()>0){

        val ds = c.as[SubjectBalanceSlim]
        ds.show()
        val thisMonth = new Date()
        val dfm = new SimpleDateFormat("yyyyMM")
        ds.orderBy("companyId","accountPeriod", "subjectCode").map(slim=>{
          System.currentTimeMillis()
          val months = monthIntval( dfm.parse(slim.accountPeriod.toString), thisMonth)
          println(s"${slim.companyId} ${slim.accountPeriod} ${slim.subjectCode}月份差：${months}")
          for(i <- 0 to months){
            //createSQL(slim)
          }
          //stmt.addBatch(createSQL(slim))
          createSQL(slim)
        }).rdd.foreachPartition(it=> {
          if(it.hasNext){
            val stmt = jdbcConnection.createStatement();
            it.foreach(stmt.addBatch)
            val batchResult = stmt.executeBatch()
            println(batchResult.toList.toString)
          }
          println("aa")
        })
      }
    })


    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop(true,  true);

  }


  def monthIntval(d1: Date, d2: Date) = {
    import java.util.Calendar
    val c1 = Calendar.getInstance
    val c2 = Calendar.getInstance

    c1.setTime(d1)
    c2.setTime(d2)
    c2.get(Calendar.MONTH) - c1.get(Calendar.MONTH)
  }


  def jdbcConnection  = {
    DriverManager.getConnection("jdbc:mysql://192.168.55.215:8066/qf_cfgdb?characterEncoding=utf8","qf_user1","hwsofti201710")
  }


  /**
    * @return
    */
  def prepareStatement = {
    val stmt = jdbcConnection.createStatement()
  }

  def createSQL(balance: SubjectBalanceSlim): String = {
    val stringBuilder = new StringBuilder("update gd_test01 set ")

    var setList = List[String]()
    if(balance.currentCreditAmount != 0){
      setList = setList :+ s"current_Credit_Amount = current_Credit_Amount + ${balance.currentCreditAmount}"
    }
    if(balance.currentCreditQty != 0){
      setList = setList :+ s"current_Credit_Qty = current_Credit_Qty + ${balance.currentCreditQty}"
    }

    if(balance.currentCreditNocarryAmount != 0){
      setList = setList :+ s"current_Credit_Nocarry_Amount = current_Credit_Nocarry_Amount + ${balance.currentCreditNocarryAmount}"
    }

    if(balance.currentDebitAmount != 0){
      setList = setList :+ s"current_Debit_Amount = current_Debit_Amount + ${balance.currentDebitAmount}"
    }

    if(balance.currentDebitQty != 0){
      setList = setList :+ s"current_Debit_Qty = current_Debit_Qty + ${balance.currentDebitQty}"
    }

    if(balance.currentDebitNocarryAmount != 0){
      setList = setList :+ s"current_Debit_Nocarry_Amount = current_Debit_Nocarry_Amount + ${balance.currentDebitNocarryAmount}"
    }
    val  sets = org.apache.commons.lang3.StringUtils.join(setList.asJava, ",")
    stringBuilder.append(sets)
      .append(s" where company_id = ${balance.companyId} and account_period = ${balance.accountPeriod/100} and subject_code = ${balance.subjectCode}")
      //.toString()
    println(stringBuilder.toString())
    stringBuilder.toString()
  }


}
