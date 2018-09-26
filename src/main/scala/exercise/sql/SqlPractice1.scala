package com.spsoft.spark.voucher

import java.util.{Date, Properties}

import com.google.gson.Gson
import com.spsoft.spark.voucher.vo.VoucherItems
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

import scala.util.parsing.json.JSONObject

/**
  * 说明
  * DataFrame[Row] 转 DataSet[Class]: DataFrame[Row].as[Class] 需要引入encoders 隐式转换 implicit val cc = Encoders.bean(classOf[VoucherItems])
  * DataSet[Class] 转 DataFrame[Row]: DataSet[Class].toDF(), 需引入sparkContext.implicits._ 调试执行DataFrame[Row].printSchema() 或 DataSet[Class].printSchema() 查看
  *
  *
  */
object SqlPractice1 {


  /**
    * kafka监听TOPICß
    * @param topics
    * @return
    */
  def listenTopic(topics: String*): InputDStream[ConsumerRecord[String, String]] = {
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(1))
    KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParmas)
    )
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
    //通过修改列名达到跟类属性名一致
    voucherDs.withColumnRenamed("VOUCHER_ITEMS_ID","voucherItemsId")
      .withColumnRenamed("VOUCHER_ID","voucherId")
      .withColumnRenamed("COMPANY_ID","companyId")
      .withColumnRenamed("VOUCHER_TIME","voucherTime")
      .withColumnRenamed("VOUCHER_ABSTRACT","voucherAbstract")
      .withColumnRenamed("SUBJECT_ID","subjectId")
      .withColumnRenamed("SUBJECT_CODE","subjectCode")
      .withColumnRenamed("SUBJECT_FULL_NAME","subjectFullName")
      .withColumnRenamed("CURRENCY_CODE","currencyCode")
      .withColumnRenamed("EXCHANGE_RATE","exchangeRate")
      .withColumnRenamed("ORIGINAL_AMOUNT","originalAmount")
      .withColumnRenamed("SUBJECT_AMOUNT","subjectAmount")
      .withColumnRenamed("VOUCHER_DIRECTION","voucherDirection")
      .withColumnRenamed("LENDING_DIRECTION","lendingDirection")
      .withColumnRenamed("SALEUNIT_NAME","saleUnitName")
      .withColumnRenamed("QTY","qty")
      .withColumnRenamed("NOTAX_ACT_PRICE","notaxActPrice")
      .withColumnRenamed("SUBJECT_SORT","subjectSort")
      //.printSchema()
        .createOrReplaceTempView("tmp2Rename")



    sparkSession.sql("select * from tmp2Rename").show(10)
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

  def main(args: Array[String]): Unit = {
    dfToDs1
  }
}
