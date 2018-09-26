package com.spsoft.spark.voucher

import java.sql.DriverManager
import java.util.{Date, Properties}

import com.spsoft.spark.voucher.KafkaVoucherConsumerOne1.kafkaParams
import com.spsoft.spark.voucher.serializer.SubjectBalanceSlimDeserializer
import com.spsoft.spark.voucher.vo._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.Column
import scala.collection.mutable
//Json解析必备
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read => sread}
//scala和java 集合相互转换
import scala.collection.JavaConverters._


/**
  *
  */
object KafkaVoucherConsumerTwo1 {

  private var streamingContext: StreamingContext = null

  val NAME_SPACE = "klhNs"

  val ZK_NODES = "192.168.55.235:9092,192.168.55.236:9092"

  val GLOBE_OFFSET_PATH =  "/spark/test/offsets"

  val GROUP_NAME = "voucherGroupTwo"

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
      Subscribe[String, String](topics, kafkaParams)
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
  def kafkaParams: Map[String,Object] = {
    Map[String, Object](
      "bootstrap.servers" -> ZK_NODES,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[SubjectBalanceSlimDeserializer],
      "group.id" -> GROUP_NAME,//GROUP_NAME,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
  }


  def sparkSession: SparkSession = {
    SparkSession.builder().appName("JdbcVoucherOperation").master("local[4]")
      .config("spark.sql.caseSensitive", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
  }

  def getTableDF(talbe: String): DataFrame = {
    //val url = "jdbc:mysql://192.168.55.215:8066/qf_accdb?characterEncoding=utf8"
    val url = "jdbc:mysql://192.168.55.211:3306/lr_taxdb?characterEncoding=utf8"
    val properties = new Properties()
    //properties.put("user","qf_user1")
    //properties.put("password","hwsofti201710")
    properties.put("user","lr_dba")
    properties.put("password","hwsoft")
    sparkSession.read.jdbc(url, talbe, properties)
  }

  def transformJson(str: String):SubjectBalanceSlim = {
    implicit val formats = DefaultFormats + new DateToLongSerializer
    sread[SubjectBalanceSlim](str)
  }

  def main(args: Array[String]): Unit = {

    val sc = sparkSession
    /**
    val receiveTopic = "TopicTwo"
    //监听kafka topic
    val stream = listenTopic(receiveTopic)
    //val sendTopic = "TopicTwo"

    val p = kafkaParams;
    println(p)
    //println(kafkaParams1)
      */
    //val sparkConf = new SparkConf().setAppName("ababa").setMaster("local[4]");
    val streamingContext = new StreamingContext(sc.sparkContext, Seconds(1))
    //import streamingContext.implicits._
    val topics = Array("TopicTwo")

    val stream = KafkaUtils.createDirectStream[String, SubjectBalanceSlim](
      streamingContext,
      PreferConsistent,
      Subscribe[String, SubjectBalanceSlim](topics, kafkaParams)
    )

      //import sc.implicits._
    implicit val formats = DefaultFormats + new DateToLongSerializer

    /**
    stream.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition(partitionOfRecords => {
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          partitionOfRecords.foreach(record=>{
            println(s"${TaskContext.get.partitionId} ${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset} ${record.toString}")
          })
        })
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })
      */

//new SQLContext()
    stream.foreachRDD(rdd=> {
//通过此项查看分区数据
      //rdd.partitioner
      //rdd.partitioner = None

      import sc.implicits._
      //rdd.toDF()
      //rdd.map(m=>m.value()).toDF()
      if(!rdd.isEmpty()){
        //offset
        val emptyNum = BigDecimal(0)
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        /**
        val df = rdd.map(m=>m.value()).toDF().groupBy("companyId","accountPeriod", "subjectCode")
          .sum( "currentDebitAmount" , "currentDebitQty","currentDebitNocarryAmount", "currentCreditAmount", "currentCreditQty","currentCreditNocarryAmount")
          .withColumnRenamed("sum(currentDebitAmount)", "currentDebitAmount")
          .withColumnRenamed("sum(currentDebitQty)", "currentDebitQty")
          .withColumnRenamed("sum(currentDebitNocarryAmount)", "currentDebitNocarryAmount")
          .withColumnRenamed("sum(currentCreditAmount)", "currentCreditAmount")
          .withColumnRenamed("sum(currentCreditQty)", "currentCreditQty")
          .withColumnRenamed("sum(currentCreditNocarryAmount)", "currentCreditNocarryAmount")
          */
          //设置关联别名
        val df = rdd.map(m=>m.value()).toDF().groupBy("companyId","accountPeriod", "subjectCode")
          .sum( "currentDebitAmount" , "currentDebitQty","currentDebitNocarryAmount", "currentCreditAmount", "currentCreditQty","currentCreditNocarryAmount")
          .withColumnRenamed("companyId", "company_id")
          .withColumnRenamed("accountPeriod", "account_period")
          .withColumnRenamed("subjectCode", "subject_code")
        //临时缓存
        df.cache()
        //df.show()
        //import sc
        /**
          * 处理插入前当前属期没有记录，生成插入记录，并补齐，到当前属期的数据
          */
        //组装过滤条件,column没有序列化
//        val condition = rdd.map(m=>m.value()).map(m=>($"company_id" === m.companyId and $"account_period" === m.accountPeriod and $"subject_code" === m.subjectCode))//.map(m=> $"(company_id = ${m.companyId} and account_period = ${m.accountPeriod} and subject_code = '${m.subjectCode}')")
//            .reduce(_ || _)

        val condition = rdd.map(m=>m.value()).map(m=> s"""(company_id = ${m.companyId} and account_period = ${m.accountPeriod} and subject_code = '${m.subjectCode}')""")
          .reduce(_ + " or " +_)
        //println(condition)
        //c.reduce(_ or _)
        //获得当前帐套，属期，科目对应的科余额表记录数据
        val dfCheckRecord = getTableDF("at_subject_balance").filter(condition).orderBy("company_id","account_period","subject_code")//.select("company_id","account_period","subject_code")
        //dfCheckRecord.show()
        /**
        val crossResult = df.join(dfCheckRecord, df.col("companyId") === dfCheckRecord.col("company_id")
          && df.col("accountPeriod") === dfCheckRecord.col("account_period")
          && df.col("subjectCode") === dfCheckRecord.col("subject_code"), "left_outer")
          .filter(dfCheckRecord.col("sequence_id").isNull)//提取关联不上的空记录
          //.select(df.col("companyId"), df.col("accountPeriod"), df.col("subjectCode"))
          .drop(dfCheckRecord.columns: _*)
*/
          //crossResult 关联不上的记录，即在科目余额表没有对应的记录
        val connectField = Seq("company_id","account_period","subject_code")
        //dfCheckRecord.columns.filter(!connectField.contains(_)).foreach(e=> print(s"${e} "))
        val crossResult = df.join(dfCheckRecord, connectField, "left_outer")
          .filter(dfCheckRecord.col("sequence_id").isNull)//提取关联不上的空记录
          //.select(df.col("companyId"), df.col("accountPeriod"), df.col("subjectCode"))
          .drop(dfCheckRecord.columns.filter(!connectField.map(_.toUpperCase()).contains(_)): _*)
//          .withColumnRenamed("sum(currentDebitAmount)", "currentDebitAmount")
//          .withColumnRenamed("sum(currentDebitQty)", "currentDebitQty")
//          .withColumnRenamed("sum(currentDebitNocarryAmount)", "currentDebitNocarryAmount")
//          .withColumnRenamed("sum(currentCreditAmount)", "currentCreditAmount")
//          .withColumnRenamed("sum(currentCreditQty)", "currentCreditQty")
//          .withColumnRenamed("sum(currentCreditNocarryAmount)", "currentCreditNocarryAmount")
          //.withColumnRenamed("company_id", "companyId")
          //.withColumnRenamed("account_period", "accountPeriod")
          //.withColumnRenamed("subject_code", "subjectCode")
        //crossResult.show()
        //crossResult.cache()
        if(crossResult.count() > 0 ){
          //跟科目信息表关联，获取科目信息表记录

          val dd = getTableDF("cd_subject_info")//.filter($"company_id" === 3591 )//.show()
          dd.createOrReplaceTempView("info")
          //crossResult.printSchema()
          //dd.printSchema()
          //dd.select("")
            /**
          val cd = (for(a <- crossResult.as[SubjectBalanceSlim]) yield {
            s"""(company_id = ${a.companyId} and subject_code = '${a.subjectCode}' ) """
          } ).reduce(_ + " or " + _ )


          crossResult.createOrReplaceTempView("cro")

          println(cd)
              */
          val subjectInfo = dd//.filter($"company_id" === 3591 && $"subject_code".isin(List("2203","22030200","1001"): _*))//.select("company_id","subject_code" )
              //.join(crossResult.select("company_id","subject_code")).where(dd.col("company_id")===crossResult.col("COMPANY_ID") and dd.col("subject_code") === crossResult.col("SUBJECT_CODE"))
              //.show()
            .join(crossResult, Seq("company_id","subject_code")).show()//.where(crossResult.col("company_id").isNotNull)
//
//          subjectInfo.show(true)
//          println(subjectInfo.count())
         // sc.sqlContext.sql(s"select * from info where ${cd}").show()

         // sc.sqlContext.sql("select a.* from info a , cro c where a.company_id = c.companyId and a.subject_code = c.subjectCode").show()
          //subjectInfo.show()


          //crossResult.join(dd.select("company_id","subject_code"), Seq("company_id","subject_code")).show()
          //getTableDF("cd_subject_info").createOrReplaceTempView("info")
          //crossResult.createOrReplaceTempView("cross")
          //sc.sqlContext.sql("select * from cross join info on info.company_id = cross.company_id and info.subject_code = cross.subject_code").show()
          //val fiels = List("company_id","subject_code")
          //val c = subjectInfo.filter($"company_id" === 3591 && $"subject_code".isin(List("2203","22030200"): _*)).select("company_id","subject_code")//.show()
          //获取最小月份，由于数据是批量发送过来，可能会出现同一公司，同一科目编码，不同属期，所以要获取最早的属期数据;2、关联科目信息表记录，获取科目初始值
          /**
          val d = crossResult.select("company_id","subject_code")//.show()
          subjectInfo.join(d,Seq("company_id","subject_code")).show()
          crossResult//.groupBy("company_id","subject_code").min("account_period")
              .join(subjectInfo, crossResult.col("company_id") === subjectInfo.col("company_id".toUpperCase())
            && crossResult.col("subject_code") === subjectInfo.col("subject_code".toUpperCase()))//Seq("company_id", "subject_code")).distinct()
            .show()
            */
//          crossResult.as[SubjectBalanceSlim].fo
          //crossResult.as[SubjectBalanceSlim].foreachPartition(partition=>{
            //partition.foreach(c=>c.)
          //})
        }
        println(crossResult.count())
        //获取指定公司，科目表初始记录
        //val subjectInfo = getTableDF("cd_subject_info").filter()


        /**
          * 更新记录
          */
        //.filter($"company_id".isin())
        //理论上df大于0
        if(df.count()>0){
          //DF转DS
          val ds = df.withColumnRenamed("sum(currentDebitAmount)", "currentDebitAmount")
            .withColumnRenamed("sum(currentDebitQty)", "currentDebitQty")
            .withColumnRenamed("sum(currentDebitNocarryAmount)", "currentDebitNocarryAmount")
            .withColumnRenamed("sum(currentCreditAmount)", "currentCreditAmount")
            .withColumnRenamed("sum(currentCreditQty)", "currentCreditQty")
            .withColumnRenamed("sum(currentCreditNocarryAmount)", "currentCreditNocarryAmount")
            .withColumnRenamed("company_id", "companyId")
            .withColumnRenamed("account_period", "accountPeriod")
            .withColumnRenamed("subject_code", "subjectCode")
            .as[SubjectBalanceSlim]
          //ds.show()
          val dd = ds.orderBy("companyId","accountPeriod", "subjectCode").map(m=>{
            //引入数字日期隐式转换
            import IntCover._
            //val l = new java.util.ArrayList[SubjectBalanceSlim]()
            //查看凭证发生日至今的月份间隔，缺失月份要先补
            //使用yield 自动生存scala Seq
            val months = m.accountPeriod.months()
            val first = months(0)
            println(months)
            println(first)
            for(i <- months) yield {
              //l.add(SubjectBalanceSlim(m.companyId, m.accountPeriod, m.subjectCode.take(a), m.currentDebitAmount, m.currentDebitQty, m.currentCreditAmount, m.currentCreditQty, m.currentDebitNocarryAmount, m.currentCreditNocarryAmount))
              //createSQL(slim)
              val unitNum = if (m.currentDebitQty >0) m.currentDebitQty else m.currentCreditQty;
              if (i == first)
                //本期发生，期初借贷增加值为0
                //SubjectBalanceSlim(m.companyId, m.accountPeriod, i.toString, m.currentDebitAmount, m.currentDebitQty, m.currentCreditAmount, m.currentCreditQty, m.currentDebitNocarryAmount, m.currentCreditNocarryAmount)
                SubjectBalanceMedium(m.companyId, i, m.subjectCode,
                  emptyNum, emptyNum, 0, //期初
                  m.currentDebitAmount, m.currentDebitQty,  m.currentDebitNocarryAmount, m.currentCreditAmount, m.currentCreditQty, m.currentCreditNocarryAmount, //本期
                  m.currentDebitAmount, m.currentCreditAmount, unitNum, //期末
                  m.currentDebitAmount, m.currentDebitQty,  m.currentDebitNocarryAmount, m.currentCreditAmount, m.currentCreditQty, m.currentCreditNocarryAmount //本年
                )
              else
              //未来的期初及期末增加值=本期发生额，未来的本期无发生额，未来的期末增加值=期末增加值，未来的本年累计增加值=本期发生额
                SubjectBalanceMedium(m.companyId, i, m.subjectCode,
                  m.currentDebitAmount, m.currentDebitAmount, unitNum, //期初
                  emptyNum, emptyNum, emptyNum, emptyNum, emptyNum, emptyNum, //本期
                  m.currentDebitAmount, m.currentDebitAmount, unitNum, //期末
                  m.currentDebitAmount, m.currentDebitQty,  m.currentDebitNocarryAmount, m.currentCreditAmount, m.currentCreditQty, m.currentCreditNocarryAmount //本年
                )

                //SubjectBalanceSlim(m.companyId, m.accountPeriod, i.toString, m.currentDebitAmount, m.currentDebitQty, m.currentCreditAmount, m.currentCreditQty, m.currentDebitNocarryAmount, m.currentCreditNocarryAmount)
            }
            //stmt.addBatch(createSQL(slim))
            //createSQL(slim)
            //l.asScala
            //spark的转换动作只对scala集合数据生效,调用 java集合的 asScala转换成scala集合
            //l.asScala
          }).flatMap(m=>m.toList)
          //通过此项查看通过一系列转换后分区数据是否能对上
          //dd.rdd.partitions
          dd.foreachPartition(ls => {
            if(!ls.isEmpty){
              //println(ls)
              //println(ls.toList)
              println()
              ls.foreach(println)
            }
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
        //保存offset
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
      //val c = rdd.toDF()
    })



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
      props.put("metadata.broker.list", "192.168.55.225:9092,192.168.55.226:9092")
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("bootstrap.servers", "192.168.55.225:9092,192.168.55.226:9092")
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
