package com.spsoft.spark.voucher

import java.sql.DriverManager
import java.util
import java.util.{Date, Properties}

import com.spsoft.spark.voucher.KafkaVoucherConsumer3.{listenTopic, transformJson}
import com.spsoft.spark.voucher.KafkaVoucherConsumerTwo.listenTopic
import com.spsoft.spark.voucher.vo._
import exercise.KafkaWordCountOffsetConsumer.{Globe_kafkaOffsetPath, client, ensureZKPathExists}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
//Json解析必备
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read => sread, write}
//scala和java 集合相互转换
import scala.collection.JavaConverters._


/**
  *
  */
object KafkaVoucherConsumerOne {

  private var streamingContext: StreamingContext = null

  val NAME_SPACE = "klhNs"

  //val ZK_NODES = "192.168.55.235:9092,192.168.55.236:9092"
  val ZK_NODES = "192.168.55.226:9092"

  val GLOBE_OFFSET_PATH =  "/spark/test/offsets"

  val GROUP_NAME = "voucherGroup"

  var kafkaProducer: KafkaProducer[String, String] = null

  val client = {
    val client = CuratorFrameworkFactory
      .builder
      .connectString(ZK_NODES)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .namespace(NAME_SPACE)
      .build()
    client.start()
    client
  }



  // 路径确认函数  确认ZK中路径存在，不存在则创建该路径
  def ensureZKPathExists(path: String)={

    if (client.checkExists().forPath(path) == null) {
      client.create().creatingParentsIfNeeded().forPath(path)
    }

  }
  // 保存offset
  def storeOffsets(offsetRange: Array[OffsetRange], groupName:String) = {
    for (o <- offsetRange){
      val zkPath = s"${GLOBE_OFFSET_PATH}/${groupName}/${o.topic}/${o.partition}"
      ensureZKPathExists(zkPath)
      // 向对应分区第一次写入或者更新Offset 信息
      println("---Offset写入ZK------\nTopic：" + o.topic +", Partition:" + o.partition + ", Offset:" + o.untilOffset)
      client.setData().forPath(zkPath, o.untilOffset.toString.getBytes())
    }
  }


  def getFromOffset(topics: Array[String], groupName:String):Map[TopicPartition, Long] = {

    // Kafka 0.8和0.10的版本差别，0.10 为 TopicPartition   0.8 TopicAndPartition
    var fromOffset: Map[TopicPartition, Long] = Map()


    val m = topics.flatMap(t=>{
      val zkTopicPath = s"${GLOBE_OFFSET_PATH}/${groupName}/${t}"
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
    * kafka监听TOPICß
    * @param topics
    * @return
    */
  def listenTopic(groupName: String, topics: String*): InputDStream[ConsumerRecord[String, String]] = {
    KafkaUtils.createDirectStream[String, String](
      streamContext,
      PreferConsistent,
      //自己存储方式
      //Subscribe[String, String](topics, kafkaParmas, getFromOffset(topics.toArray, groupName))
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
      "bootstrap.servers" -> ZK_NODES,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> GROUP_NAME,
      //"auto.offset.reset" -> "earliest",
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
    val receiveTopic = "TopicOne"
    //监听kafka topic
    val stream = listenTopic(receiveTopic)
    val sendTopic = "TopicTwo"



    stream.foreachRDD(rdd=>{
      //if(!rdd.isEmpty()){
        //记下当前offset
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        rdd.foreachPartition(partitionOfRecords => {
          implicit val formats = DefaultFormats + new DateToLongSerializer
          partitionOfRecords.map(record=> {
            transformJson(record.value())
          }).flatMap(v=>{
            //定义scala的List
            var l = List[SubjectBalanceSlim]()
            //分解凭证细表
            v.items.map(i=>{
              val debit = if (i.lendingDirection == 1) i.subjectAmount else BigDecimal(0)
              val debitQty = if (i.lendingDirection == 1) i.qty else BigDecimal(0)
              //贷金额
              val credit = if (i.lendingDirection == 2) i.subjectAmount else BigDecimal(0)
              val creditQty = if (i.lendingDirection == 2) i.qty else BigDecimal(0)
              SubjectBalanceSlim(v.companyId, v.accountPeriod, i.subjectCode, debit, debitQty, credit, creditQty, 0, 0)
            })
          }).flatMap(m=> {
            //分解凭证细表的科目层级,如100020003000分解成100020003000,10002000,1000三条明细记录
            var l = List[SubjectBalanceSlim]()
            for (a <- 1 to m.subjectCode.length if a % 4 == 0) {
              l = l :+ SubjectBalanceSlim(m.companyId, m.accountPeriod, m.subjectCode.take(a), m.currentDebitAmount, m.currentDebitQty, m.currentCreditAmount, m.currentCreditQty, m.currentDebitNocarryAmount, m.currentCreditNocarryAmount)
            }
            l
            //发送到kafka做下一步处理
          }).foreach(r => {//此处无法保证事务唯一性
            println(r.toString)
            producer.send(new ProducerRecord[String, String](sendTopic, s"${r.companyId}${r.accountPeriod}${r.subjectCode.take(4)}",write(r)))
          })
        })
        //保存offset
        //storeOffsets(offsetRanges, GROUP_NAME)
      //}
    })

    /**
    listenTopic("TopicA").map(dstream=> {
      //此处有offset
      //val offsetRanges = dstream.asInstanceOf[HasOffsetRanges].offsetRanges
      println(dstream)
      //转换JSON字符串成对象
      transformJson(dstream.value())

    })
      .persist() //如果察觉监听处有多次打印，可以在此处缓存
      .flatMap(v=>{
      // 定义scala的List
      var l = List[SubjectBalanceSlim]()
      //分解凭证细表
      v.items.map(i=>{
        val debit = if (i.lendingDirection == 1) i.subjectAmount else BigDecimal(0)
        val debitQty = if (i.lendingDirection == 1) i.qty else BigDecimal(0)
        //贷金额
        val credit = if (i.lendingDirection == 2) i.subjectAmount else BigDecimal(0)
        val creditQty = if (i.lendingDirection == 2) i.qty else BigDecimal(0)
        SubjectBalanceSlim(v.companyId, v.accountPeriod, i.subjectCode, debit, debitQty, credit, creditQty, 0, 0)
      })
    }).flatMap(m=> {
      //分解凭证细表的科目层级,如100020003000分解成100020003000,10002000,1000三条明细记录
      var l = List[SubjectBalanceSlim]()
      for (a <- 1 to m.subjectCode.length if a % 4 == 0) {
        l = l :+ SubjectBalanceSlim(m.companyId, m.accountPeriod, m.subjectCode.take(a), m.currentDebitAmount, m.currentDebitQty, m.currentCreditAmount, m.currentCreditQty, m.currentDebitNocarryAmount, m.currentCreditNocarryAmount)
      }
      l
      //分组，具体到科目，此处分组时为了分区，将相同数据分到同一区域
    }).map(m=> (s"${m.companyId}${m.accountPeriod}${m.subjectCode}", m))
        .groupByKey(new MyPartitioner(3))//关键，分组并重新分区
      //将分组后数据解开
      .flatMap({_._2.toList})
      //数据落地，存入数据库
      .foreachRDD(rdd=> {
//通过此项查看分区数据
      rdd.partitioner
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
        //val thisMonth = new Date()
        //val dfm = new SimpleDateFormat("yyyyMM")
        val dd = ds.orderBy("companyId","accountPeriod", "subjectCode").map(m=>{
          //val oldMonth = dfm.parse(slim.accountPeriod.toString)
          //val months = monthIntval( oldMonth, thisMonth)
          //引入数字日期隐式转换
          import IntCover._
          //val months = dfm.parse(slim.accountPeriod.toString).getTime
          //println(s"${slim.companyId} ${slim.accountPeriod} ${slim.subjectCode}月份差：${months}")
          val l = new util.ArrayList[SubjectBalanceSlim]()
          //凭证发生月至今有几个月，将之后月份补齐
          for(i <- m.accountPeriod.months()){
            //l.add(SubjectBalanceSlim(m.companyId, m.accountPeriod, m.subjectCode.take(a), m.currentDebitAmount, m.currentDebitQty, m.currentCreditAmount, m.currentCreditQty, m.currentDebitNocarryAmount, m.currentCreditNocarryAmount))
            //createSQL(slim)
            l.add(SubjectBalanceSlim(m.companyId, m.accountPeriod, i.toString, m.currentDebitAmount, m.currentDebitQty, m.currentCreditAmount, m.currentCreditQty, m.currentDebitNocarryAmount, m.currentCreditNocarryAmount))
          }
          //stmt.addBatch(createSQL(slim))
          //createSQL(slim)
            //l.asScala
          //spark的转换动作只对scala集合数据生效,调用 java集合的 asScala转换成scala集合
          l.asScala
        }).flatMap(m=>m.toList)
        //通过此项查看通过一系列转换后分区数据是否能对上
        dd.rdd.partitions
        dd.foreachPartition(ls => {
          println(ls)
          println(ls.toList)
        })
          /**.rdd.foreachPartition(it=> {
          if(it.hasNext){
            val stmt = jdbcConnection.createStatement();
            it.foreach(stmt.addBatch)
            val batchResult = stmt.executeBatch()
            println(batchResult.toList.toString)
          }
          println("aa")
        })
            */
      }
    })
*/
    // 开启监听
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
