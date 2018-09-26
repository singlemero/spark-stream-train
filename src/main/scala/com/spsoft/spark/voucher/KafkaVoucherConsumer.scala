package com.spsoft.spark.voucher

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Date, Properties, StringJoiner}

import breeze.linalg.max
import com.google.gson.Gson
import com.spsoft.spark.voucher.vo._
import javax.security.auth.Subject
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

import scala.util.parsing.json.JSONObject
import org.json4s.jackson.Serialization.{write, read => sread}
import scala.collection.JavaConverters._


/**
  *
  */
object KafkaVoucherConsumer {

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
      "bootstrap.servers" -> "192.168.55.226:9092",
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


  /**
    * DataFrame 转 DataSet
    */
  def dfToDs1 = {
    val voucherItemsDF = getTableDF("at_voucher_items")
    voucherItemsDF.createOrReplaceTempView("tmp1");//创建临时表



    //执行某些SQL,返回datafram
    val voucherDf = sparkSession.sql("select * from tmp1 where company_id = 1")

    /**
      * 引入隐式转换 encoders， 不引入无法转换DS
      */
    implicit val cc = Encoders.bean(classOf[VoucherItems])

    //转换成dataSet，但是SQL字段依旧是 数据库字段
    val voucherDs: Dataset[VoucherItems] = voucherDf.as[VoucherItems]

    voucherDs.printSchema()
    voucherDs.createOrReplaceTempView("tmp2")

    sparkSession.sql("select * from tmp2").show(10)
  }

  /**
    * 通过自定义字段方式实现df转ds， 不依赖具体vo 实体
    */
  def dfToDs2 = {
    val voucherItemsDF = getTableDF("at_voucher_items")
    voucherItemsDF.createOrReplaceTempView("tmp2");//创建临时表

    //执行某些SQL,返回datafram
    val voucherDf = sparkSession.sql("select * from tmp2 where company_id = 1")

    //implicit val beas = Encoders.javaSerialization(classOf[VoucherItems])
    val sc = sparkSession
    import sc.implicits._//引入转换
    /**
      * 先转RDD，在操作
      * 为什么如此，参考以下答案部分 https://stackoverflow.com/questions/37706420/how-to-create-a-custom-encoder-in-spark-2-x-datasets
      * 这里可以对rdd内容进行修改，添加字段或修改字段，修改顺序，修改值
      */
    val voucherDs  = voucherDf.rdd.map(r=> Row(
      r(0),r(1),r(2),r(3),r(4),r(5),r(6),r(7),r(8),r(9),r(10),r(11),r(12),r(13),r(14),r(15),r(16),r(17)
    ))


    val fields = Array(StructField("voucherItemsId", StringType, nullable = true),
      StructField("voucherId", StringType, nullable = true),
      StructField("companyId", IntegerType, nullable = true),
      StructField("voucherTime", DateType, nullable = true),
      StructField("voucherAbstract", StringType, nullable = true),
      StructField("subjectId", LongType, nullable = true),
      StructField("subjectCode", StringType, nullable = true),
      StructField("subjectFullName", StringType, nullable = true),
      StructField("currencyCode", StringType, nullable = true),
      StructField("exchangeRate", DoubleType, nullable = true),
      StructField("originalAmount", DoubleType, nullable = true),
      StructField("subjectAmount", DoubleType, nullable = true),
      StructField("voucherDirection", IntegerType, nullable = true),
      StructField("lendingDirection", IntegerType, nullable = true),
      StructField("saleUnitName", StringType, nullable = true),
      StructField("qty", DoubleType, nullable = true),
      StructField("notaxActPrice", DoubleType, nullable = true),
      StructField("subjectSort", StringType, nullable = true))

    val schema = StructType(fields)

    //这里 voucherDf.rdd 等价于 voucherDs :仅当voucherDs没有对字段增加或修改的情况
    val myDf = sparkSession.createDataFrame(voucherDf.rdd, schema)
    //字段是自定义的字段
    myDf.printSchema()
  }

  def join1 = {
    val sc = sparkSession
    import sc.implicits._
    val df1 = sc.sparkContext.parallelize(Array(
      ("one", "A", 1), ("one", "B", 2), ("two", "A", 3), ("two", "B", 4)
    )).toDF("key1", "key2", "value2")
//    df1.show()

    val df2 = sc.sparkContext.parallelize(Array(
      ("one", "A", 5), ("two", "A", 6)
    )).toDF("key1", "key2", "value2")
//    df2.show()
    //字段名称相同，Seq 关联列合并
    df1.join(df2, Seq("key1", "key2"), "left_outer")//.show()
    //字段名称相同，=== 关联列不合并
    df1.join(df2, df1("key1") === df2("key1") && df1("key2") === df2("key2"))//.show()

    val df3 = sc.sparkContext.parallelize(Array(
      ("one", "C", 5), ("two", "D", 6), ("one", "A", 33)
    )).toDF("F1", "F2", "value3")

    //字段名称不同
    df1.join(df3, df1.col("key1") === df3.col("F1") && df1.col("key2") === df3.col("F2"))
      //.show()

    //字段名称不同,
    //重命名列1
    df3.select($"F1" as("key1"), $"F2" as("key2"))//.show()
    //重命名列2
    df3.selectExpr("F1 as key1", "F2 as key2")//.show()

    df1.join(df3.select($"F1" as("key1"), $"F2" as("key2")), Seq("key1", "key2")).select("key1", "key2")//.show

    //添加列,新列为指定列的复制列
    val df3copy = df3.withColumn("F3", $"F2")

    df1.join(df3copy.select($"F1" as("key1"), $"F2" as("key2"), $"value3", $"F3" as("value2")), Seq("key1", "key2")).select("key1", "key2")//.show()
    //关联表，重复列名，通过指定表别名实现指定具体列
    df1.alias("dd1").join(df3copy.select($"F1" as("key1"), $"F2" as("key2"), $"value3", $"F3" as("value2")).alias("dd3"), Seq("key1", "key2"))
        .select("dd1.value2","dd3.value2").show()
    sc.stop()
  }


  def transformJson(str: String): Voucher = {
    implicit val formats = DefaultFormats + new DateToLongSerializer
    sread[Voucher](str)
  }

  def main(args: Array[String]): Unit = {
    /**
    listenTopic("TopicA").map(dstream => dstream.value()).foreachRDD(rdd=> rdd.foreachPartition { partitionOfRecords =>
      partitionOfRecords.foreach(println)
    })
      */

    val sc = sparkSession
    import sc.implicits._
    /**
    listenTopic("TopicA").map(dstream=> {
      transformJson(dstream.value())

    })

      //合并同一公司同一属期数据
      .map(v => (String.join("", v.companyId.toString, v.accountPeriod.toString), v)).groupByKey()
        .map(m=> {
          val ni = m._2.flatMap(v=> v.items)
          val nc = m._2.map(v=> v.codes).reduce[String](String.join(",",_,_))
          Voucher(null, m._2.head.companyId, m._2.head.accountPeriod, nc, ni.toList)
        })


      //分解编码

      .foreachRDD(rdd => {
      rdd.foreachPartition(f => {
        f.foreach(v=> {
          //处理编码
          val codes = v.items.map(m => {
            val sb = new StringJoiner(",")
            for(a <- 1 to m.subjectCode.length if a % 4 == 0){
              sb.add("'"+m.subjectCode.take(a)+"'")
            }
            sb.toString
          }).reduce[String](String.join(",", _, _))
          //属期，由于数据来源自凭证表，凭证表时间格式为yyyyMMdd，需去掉后两位
          val period = v.accountPeriod/100
          //查询条件，TODO 测试,理论上不需要获取此数据
          val query = s"company_id = ${v.companyId} and account_period= ${period} and subject_code in (${codes})"
          //println(query)
          //获取表数据 TODO 测试，理论上不需要获取此数据
          val record = getTableDF("at_subject_balance").filter(query)
            .count()
          //println(record.toString)

          //构建数据,(科目编码,[借金额，贷金额])
          //分解科目编码，将多级科目编码分解，分解后每级数据一致
          v.items.flatMap(m=> {
            var tmpzz = new util.ArrayList[SubjectBalanceSlim]()
            var l = List[SubjectBalanceSlim]()
            for (a <- 1 to m.subjectCode.length if a % 4 == 0) {
              //借金额
              val debit = if (m.lendingDirection == 1) m.subjectAmount else BigDecimal(0)
              val debitQty = if (m.lendingDirection == 1) m.qty else BigDecimal(0)
              //贷金额
              val credit = if (m.lendingDirection == 2) m.subjectAmount else BigDecimal(0)
              val creditQty = if (m.lendingDirection == 2) m.qty else BigDecimal(0)
//              tmpzz.add(SubjectBalanceSlim(v.companyId, v.accountPeriod, m.subjectCode.take(a), debit, debitQty, credit, creditQty, 0, 0))
              l = l :+ SubjectBalanceSlim(v.companyId, v.accountPeriod, m.subjectCode.take(a), debit, debitQty, credit, creditQty, 0, 0)
            }
            //Seq(tmpzz)
            l

            //合并科目编码数据，并统计实际的借贷金额数量
          }).groupBy(m=> m.subjectCode).map(m => {
            val c = SubjectBalanceSlim(0, 0, null, 0, 0, 0, 0, 0, 0)
            //c.subjectCode = "aaa"
            m._2.toDF().select("accountPeriod"->sun)
          })

          v.items.map(m=> {
            (m.subjectCode, Array(if(m.lendingDirection == 1) m.subjectAmount else 0, if(m.lendingDirection == 2) m.subjectAmount else 0))
          })//.grou
        })
      })




    })
        */
    /**
    val filter = "company_id = resources3591 and account_period= 201801 and subject_code in ('2203','22030200','6001','60010105','2221','22210001','222100010002','2203','22030126','6001','60010104','2221','22210001','222100010002')"
    getTableDF("at_subject_balance").filter(filter).show()
  */




    listenTopic("TopicA").map(dstream=> {
      transformJson(dstream.value())

    }).flatMap(v=>{
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
    })//.map(v => (String.join("", v.companyId.toString, v.accountPeriod.toString), v)).groupByKey()
      /**
        .map(v=> {
          import sc.sqlContext.implicits._
          println(v._2.toList)
          val d = v._2.toList.toDF()
            .groupBy("accountPeriod")
            .agg(Map("currentDebitAmount" -> "sum", "currentDebitQty" -> "sum",
              "currentCreditAmount"->"sum", "currentDebitNocarryAmount"->"sum",
        "currentDebitNocarryAmount"->"sum", "currentDebitNocarryAmount"-> "sum"))
          d.show()
          d

        }).foreachRDD(rdd=> rdd.foreachPartition(p=>{
      p.foreach(u=> u.show())
      */
        .foreachRDD(rdd=> {

      import sc.sqlContext.implicits._

      //println(rdd.toDF().count())
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

          /**.collect().foreach(
          stmt.addBatch(_)
        )
            */

      }

    })


      /**.map(v => {
      val code = v.items.flatMap(m => {
        val sb = new StringJoiner(",")
        for(a <- 1 to m.subjectCode.length if a % 4 == 0){
          sb.add(m.subjectCode.take(a))
        }
        sb.toString
      }).reduce[String](String.join(",", _, _))

      v
    }).map(v => {
      getTableDF("at_subject_balance").filter(s"company_id = ${v.companyId} and account_period= ${v.accountPeriod} and subject_code in (${v.codes})")

    }).foreachRDD(r => r.foreach(s => s.foreach(a=> printf(a.toString()))))
*/
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
    /**
      * val url = "jdbc:mysql://192.168.55.215:8066/qf_accdb?characterEncoding=utf8"
      * val properties = new Properties()
      *     properties.put("user","qf_user1")
      *     properties.put("password","hwsofti201710")
      *     sparkSession.read.jdbc(url, talbe, properties)
      */
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
