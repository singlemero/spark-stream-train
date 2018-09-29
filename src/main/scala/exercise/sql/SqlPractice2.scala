package exercise.sql

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.spsoft.spark.voucher.serializer.DateToLongSerializer
import com.spsoft.spark.voucher.vo.VoucherItems
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

object SqlPractice2 {

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

  def producer: KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("metadata.broker.list", "192.168.55.226:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("bootstrap.servers", "192.168.55.226:9092")
    //    props.put("partitioner.class", "com.fortysevendeg.biglog.SimplePartitioner")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("producer.type", "async")
    props.put("request.required.acks", "1")
    new KafkaProducer[String, String](props)
  }


  def main(args: Array[String]): Unit = {
    var messageNo = 1
    val topic = "TopicA"
    val spark =  sparkSession

    import spark.implicits._

    val voucherItemDF = getTableDF("at_voucher_items")
    voucherItemDF.createOrReplaceTempView("items")
    //voucherItemDF.printSchema()


    val voucherDF = getTableDF("at_voucher")
    voucherDF.createOrReplaceTempView("voucher")
    //    voucherDF.printSchema()
    //implicit val d = Encoders.kryo(classOf[Date]);
    //voucherDF.show(10)

    //implicit val cc = Encoders.kryo(classOf[VoucherItems])

    //相同操作
    /**
    voucherDF.filter($"company_id" === 3591 && 1 == 1 )
    val voucherDs = {
      voucherDF.filter("company_id = 3591 and 1=1") //过滤指定company_id
        .alias("v") //指定别名
      //内关联,由于voucher主表和子表均有voucher_id,company_id ，通过子表voucher_id 即可知道主单，本处可不关联表
        //.join(voucherItemDF.filter("company_id = 3591").select("voucher_id").alias("vi"), Seq("voucher_id"), "inner")

        //.show(1)
      //.rdd.groupBy(f=> f.getAs[Long]("VOUCHER_ID")).foreach(f=> f._2.foreach(println));
      //.as[VoucherItems]}

    }
      */

    /**
      *
      * @param voucherItemsId 凭证细单ID
      * @param voucherId 凭证主单ID
      * @param companyId 公司ID
      * @param voucherTime 凭证日期（分区键）
      * @param voucherAbstract 凭证摘要
      * @param subjectId 科目ID
      * @param subjectCode 科目代码
      * @param subjectFullName 科目全称
      * @param currencyCode 币别编码
      * @param exchangeRate 汇率
      * @param originalAmount 原币金额
      * @param subjectAmount 发生金额
      * @param voucherDirection 凭证借贷方向（1：借、2：贷）
      * @param lendingDirection 科目表的借贷方向（1：借、2：贷）
      * @param saleUnitName 单位名称
      * @param qty 数量
      * @param notaxActPrice 不含税终计单价
      * @param subjectSort 排列序号
      */
    val ds = voucherItemDF.filter($"company_id" === 3591).select(
      $"VOUCHER_ITEMS_ID" as("voucherItemsId"),
      $"VOUCHER_ID" as("voucherId"),
      $"COMPANY_ID" as("companyId"),
      $"VOUCHER_TIME" as("voucherTime"),
      $"VOUCHER_ABSTRACT" as("voucherAbstract"),
      $"SUBJECT_ID" as("subjectId"),
      $"SUBJECT_CODE" as("subjectCode"),
      $"SUBJECT_FULL_NAME" as("subjectFullName"),
      $"CURRENCY_CODE" as("currencyCode"),
      $"EXCHANGE_RATE" as("exchangeRate"),
      $"ORIGINAL_AMOUNT" as("originalAmount"),
      $"SUBJECT_AMOUNT" as("subjectAmount"),
      $"VOUCHER_DIRECTION" as("voucherDirection"),
      $"LENDING_DIRECTION" as("lendingDirection"),
      $"SALEUNIT_NAME" as("saleUnitName"),
      $"QTY" as("qty"),
      $"NOTAX_ACT_PRICE" as("notaxActPrice"),
      $"SUBJECT_SORT" as("subjectSort")
    ).as[VoucherItems]


    //    ds.map(case VoucherItems => )
    //    ds.rdd.groupBy(f=> f.voucherId.toString())
    //      .map(k=> printf(k._1))
    //ds.rdd.

    val map = ds.rdd.groupBy(f=> f.voucherId).collectAsMap()
    //    parse()
    /**jackson  做法
val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    map.mapValues(f=> {
      val out = new StringWriter
      mapper.writeValue(out, f.toList)
      out.toString()
    } ).map(b=> b._2).foreach(println)
      */
    /**json4sa*/
    implicit val formats = DefaultFormats + new DateToLongSerializer
    val messages = map.mapValues(f=> write(f.toList)).map(b=> b._2)//.foreach(println)



    //t.foreach(k  => printf(k._1.toString()))
    val sechema = StructType(StructType(Seq(StructField("id",StringType,true),StructField("items",Encoders.kryo(classOf[VoucherItems]).schema,true))))
    //implicit val c = RowEncoder(sechema)

    //    ds.toDF().groupBy("voucherId").count()

    //val l = ds.groupByKey(t=> t.voucherId).count().show()
    //val l = ds.groupByKey(k=> k.voucherId.toString()).mapValues(m=> printf(m.voucherId.toString()))
    printf("")
    //val voucherDs2 = voucherItemDF.filter($"company_id" === 3591).as[VoucherItems]

    //voucherDs.rdd.collect().foreach(i=> printf(i.getCurrencyCode))
    //.rdd.map(r=> (r(0).asInstanceOf[Long], r)).groupByKey()
    //.foreach(f=> printf(f._1.asInstanceOf[Long].toString))
    //    sc.sql("select * from items where company_id = 3591").show(10)
    //sc.sql("select * from items join voucher where items.voucher_id = voucher.voucher_id").show(10)



    /** */
    for(msg <- messages){
      //val messageStr = new String("Message_" + messageNo)
      System.out.println("Send:" + msg)
      producer.send(new ProducerRecord[String, String](topic, msg))
      messageNo += 1
      try
        TimeUnit.MILLISECONDS.sleep(400)
      catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
    }

    spark.stop()
    //val m = sparkSession.sparkContext.parallelize(Array(("A","B","C"),("A","D","E"),("B","1","2"),("B","3","4"),("C","AA","BB")))
    //.toDF().rdd.groupBy(p=> p(0).toString).foreach(f => f._2.foreach(println))
    //m.iterator.foreach(i => println(i._1))
    //    m.foreach(f=> f._2.foreach(println))

    /**
    val conf = new SparkConf().setAppName("JdbcOperation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val properties = new Properties()
    properties.put("user","qf_user1")
    properties.put("password","hwsofti201710")
    val url = "jdbc:mysql://192.168.55.215:8066/qf_accdb?characterEncoding=utf8"
    //读表

    val voucher_itme = sqlContext.read.jdbc(url,"at_voucher_items",properties)
    voucher_itme.createOrReplaceTempView("gg");
    //voucher_itme.select("select * from gg").limit(100);
    //val df = sqlContext.sql("select * from gg where company_id = 1")//.show(100)
*/
    //readFile(spark.sparkContext)

  }

  def readFile(sparkContext: SQLContext) = {
    import sparkContext.implicits._
    val person  = Seq(Person(1, "lili", 7), Person(2, "John", 8), Person(3, "Zoom", 7)).toDS();

    val cls = Seq(ZClass(11, "语文", 1), ZClass(12, "数学", 2)).toDS()

    person.join(cls, person.col("id") === cls.col("pId")).drop(person.col("id")).show()

  }
}
