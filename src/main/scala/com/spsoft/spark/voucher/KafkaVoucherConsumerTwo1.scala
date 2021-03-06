package com.spsoft.spark.voucher

import java.sql.Date
import java.util.Properties

import com.spsoft.common.utils.IdWorker
import com.spsoft.spark.utils.KafkaProperties._
import com.spsoft.spark.utils._
import com.spsoft.spark.voucher.serializer.{DateToLongSerializer, SubjectBalanceSlimDeserializer}
import com.spsoft.spark.voucher.vo._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.TaskContext
import org.apache.spark.sql._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.{Failure, Try}
//Json解析必备
import org.json4s.DefaultFormats
//scala和java 集合相互转换
import scala.collection.JavaConverters._
//引入sql聚合函数
import org.apache.spark.sql.functions.{col, expr}
import com.spsoft.spark.utils.DatabaseProperties.{meta => JMETA}
/**
  *
  */
object KafkaVoucherConsumerTwo1 {

  private val LOG = LoggerFactory.getLogger(KafkaVoucherConsumerTwo.getClass)

  val GROUP_NAME = "voucherGroupTwo"
  //val GROUP_NAME = "klhTest"

  val IDWORK_NAME = "SUBJECT_BALANCE"

  val TARGET_TABLE = "GD_TEST01"

  val REPARTITION = 6;


//  val DATABASE_URL = List(
//    Array("jdbc:mysql://192.168.55.211:3306/lr_taxdb?characterEncoding=utf8&useSSL=false","lr_dba","hwsoft"),
//    Array("jdbc:mysql://192.168.55.212:3306/lr_taxdb?characterEncoding=utf8&useSSL=false","lr_dba","hwsoft")
//  )
// 测试
  val DATABASE_URL = List(
    Array("jdbc:mysql://192.168.55.201:3306/lr_taxdb?characterEncoding=utf8&useSSL=false","lr_dba","hwsoft"),
    Array("jdbc:mysql://192.168.55.202:3306/lr_taxdb?characterEncoding=utf8&useSSL=false","lr_dba","hwsoft")
  )
  /**
    * kafka参数
    * @return
    */
  def kafkaParams: mutable.Map[String,Object] = {
    get(Map[String, Object]("value.deserializer" -> classOf[SubjectBalanceSlimDeserializer], "group.id" -> GROUP_NAME)).asScala
  }



  def getTableDF(talbe: String, sparkSession: SparkSession): DataFrame = {
    val ds = Array("url", "username", "password").map(DataSourceProperties.get)
    val properties = new Properties()
    properties.put("user",    ds(1))
    properties.put("password",ds(2))
    properties.put("numPartitions", "8")
    sparkSession.read.jdbc(ds.head, talbe, properties)
  }

  def getTableDF(talbe: String, dsArray: Array[String], sparkSession: SparkSession): DataFrame = {
    val properties = new Properties()
    properties.put("user",    dsArray(1))
    properties.put("password",dsArray(2))
    properties.put("numPartitions", REPARTITION.toString)
    sparkSession.read.jdbc(dsArray.head, talbe, properties)
  }

  def getTableDF(talbe: String, briefMeta: BriefMeta, sparkSession: SparkSession): DataFrame = {
    val properties = new Properties()
    properties.put("user",    briefMeta.user)
    properties.put("password", briefMeta.password)
    properties.put("numPartitions", REPARTITION.toString)
    sparkSession.read.jdbc(briefMeta.url, talbe, properties)
  }

  /**
    * 如果没有会计属期开始记录（最小会计属期）或会计属期开始记录小于未来一个月的属期
    * 例1：插入记录属期201808，当前最小201810，从201808补到201809
    * 例2：插入记录属期201808，没有最小属期，当前最新201811，从201808补到201811
    * @param dataFrame dstream和余额表交集后的dataframe
    * @param infoDF 科目信息表
    * @param sparkSession
    */
  def before(dataFrame: DataFrame, infoDF: DataFrame, sparkSession: SparkSession) = {
    import com.spsoft.spark.hint.DataFrameHints._
    import com.spsoft.spark.hint.DateHints._
    import sparkSession.implicits._
    implicit val BriefVoEncoder = org.apache.spark.sql.Encoders.kryo[SubjectInfoBriefVo]
//    println(dataFrame.rdd.partitions.size)
    val nextYM = new Date(System.currentTimeMillis()).nextMonth()
    val emptyRecord = dataFrame
      .filter(col("accountPeriodStart").isNull || (col("accountPeriodStart").isNotNull && col("accountPeriod") < col("accountPeriodStart")))
      .selectExpr("companyId as company_id","subjectCode as subject_code", "accountPeriod", s"(case when accountPeriodStart is null then ${nextYM} else accountPeriodStart end) as accountPeriodEnd")

    if(!emptyRecord.rdd.isEmpty()){
      LOG.info("/****************************empty 或者凭证所属期小于最小科目余额业务属期******************************/")
      //从科目信息表查找指定的公司科目信息
      val querySubjectInfoStr = emptyRecord.rdd.map(row => {
        s"""(company_id = ${row.getAs[Int]("company_id")} and subject_code = '${row.getAs[String]("subject_code")}')"""
      }).reduce(_ + " or " + _)

      val rd = infoDF
        .where(querySubjectInfoStr)
        .join(emptyRecord, Seq("company_id", "subject_code"))
        .convertNames()
        .repartition(REPARTITION, $"companyId",$"subjectCode")
        .as[SubjectInfoBrief]
//      rd.rdd.sortBy(_.accountPeriod).p
      rd.foreachPartition(p => {
        val c = p.flatMap(f => {
          val idWorker = IdWorker.getInstance(IDWORK_NAME, TaskContext.get.partitionId)
          buildInsertBalanceHead(f, idWorker)
        })

        Try(insetBalanceInitial(c)) match {
          case Failure(ex) => {
            LOG.error(s"插入指定公司科目余额表数据发生异常！(Head)\\r\\n ${c.toSeq}")
            ex.printStackTrace()
          }
          case _ => None
        }
      })
    }
  }

  /**
    * 当前记录大于最大属期或当前最大属期小于最新，从最大属期补到最新 ，获取最大会计属期记录补到最新，最大会计属期不补，例：最大属期201810，当前最新201811，只补201811
    * @param dataFrame dstream和余额表交集后的dataframe
    * @param balanceDF 科目余额表
    * @param sparkSession
    */
  def after(dataFrame: DataFrame, balanceDF: DataFrame, sparkSession: SparkSession) = {
    import com.spsoft.spark.hint.DataFrameHints._
    import com.spsoft.spark.hint.DateHints._
    import sparkSession.implicits._
//    println(dataFrame.rdd.partitions.size)
    val currentYM = new Date(System.currentTimeMillis()).month()
    val fixTailRecord = dataFrame
      .filter(col("accountPeriodStart").isNotNull && (col("accountPeriod") > col("accountPeriodEnd")) || col("accountPeriodEnd") < currentYM
    ).selectExpr("companyId as company_id","subjectCode as subject_code", "accountPeriodEnd", s"${currentYM} as accountPeriodNow")
    //fixTailRecord.show()
    if(!fixTailRecord.rdd.isEmpty()){
      LOG.info("/****************************tail******************************/")
      //获取科目余额表指定公司、科目、科目的记录
      val queryBalanceStr = fixTailRecord.rdd.map(row => {
        s"""(company_id = ${row.getAs[Int]("company_id")} and subject_code = '${row.getAs[String]("subject_code")}') and account_period = ${row.getAs[Int]("accountPeriodEnd")}"""
      }).reduce(_ + " or " + _)

      val result = balanceDF.where(queryBalanceStr)

      result.convertNames()
        .as[SubjectBalance]
        .repartition(REPARTITION, $"companyId",$"subjectCode")
        .foreachPartition(p => {
          if(!p.isEmpty){
            val c = p.flatMap(f => {
              val idWorker = IdWorker.getInstance(IDWORK_NAME, TaskContext.get.partitionId)
              //忽略最大属期
              buildInsertBalanceTail(f, idWorker,true)
            })
            Try(insetBalanceInitial(c)) match {
              case Failure(ex) => {
                LOG.error(s"插入指定公司科目余额表数据发生异常！\\r\\n ${c.toSeq}")
                ex.printStackTrace()
              }
              case _ => None
            }
          }
        })
    }
  }

  /**
    * 更新
    * @param dstreamDF spark stream 数据段的DataFrame
    * @param balanceDF 科目余额表
    * @param sparkSession
    */
  def updateR (dstreamDF: DataFrame, balanceDF: DataFrame, sparkSession: SparkSession) = {
    import com.spsoft.spark.hint.IntHints._
    import sparkSession.implicits._

    //理论上df大于0
    if(!dstreamDF.rdd.isEmpty()){
      LOG.info("/****************************update******************************/")
      //DF转DS
      val ds = dstreamDF.as[SubjectBalanceSlim]
      val emptyNum = BigDecimal(0)
      //ds.show()
      implicit val rowEncoder = Encoders.kryo[Array[Any]]
      val dd = ds.orderBy("companyId","accountPeriod", "subjectCode")
        .repartition(REPARTITION,$"companyId",$"subjectCode")//重新分区
        .flatMap(m=>{
        //val l = new java.util.ArrayList[SubjectBalanceSlim]()
        //查看凭证发生日至今的月份间隔，缺失月份要先补
        //使用yield 自动生存scala Seq
        val months = m.accountPeriod.toYm()
        LOG.warn(s"find months start:${m} ${months}")
        val first = months(0)
        val year = first/100
        for(i <- months) yield {
          val unitNum = if (m.currentDebitQty != null && m.currentDebitQty >0) m.currentDebitQty else m.currentCreditQty;
          if (i == first)
          //本期发生，期初借贷增加值为0
            SubjectBalanceMedium(m.companyId, i, m.subjectCode,
              emptyNum, emptyNum, emptyNum, //期初
              m.currentDebitAmount, m.currentDebitQty,  m.currentDebitNocarryAmount, m.currentCreditAmount, m.currentCreditQty, m.currentCreditNocarryAmount, //本期
              m.currentDebitAmount, m.currentCreditAmount, unitNum, //期末
              m.currentDebitAmount, m.currentDebitQty,  m.currentDebitNocarryAmount, m.currentCreditAmount, m.currentCreditQty, m.currentCreditNocarryAmount //本年
            )
          else
          //未来的期初及期末增加值=本期发生额，未来的本期无发生额，未来的期末增加值=期末增加值，未来的本年累计增加值=本期发生额
            if(year == i/100){
              SubjectBalanceMedium(m.companyId, i, m.subjectCode,
                m.currentDebitAmount, m.currentCreditAmount, unitNum, //期初
                emptyNum, emptyNum, emptyNum, emptyNum, emptyNum, emptyNum, //本期
                m.currentDebitAmount, m.currentCreditAmount, unitNum, //期末
                m.currentDebitAmount, m.currentDebitQty,  m.currentDebitNocarryAmount, m.currentCreditAmount, m.currentCreditQty, m.currentCreditNocarryAmount //本年
              )
            }else{
            //不同年份，本年借贷内容为0
            SubjectBalanceMedium(m.companyId, i, m.subjectCode,
              m.currentDebitAmount, m.currentCreditAmount, unitNum, //期初
              emptyNum, emptyNum, emptyNum, emptyNum, emptyNum, emptyNum, //本期
              m.currentDebitAmount, m.currentCreditAmount, unitNum, //期末
              emptyNum, emptyNum,  emptyNum, emptyNum, emptyNum, emptyNum //本年
            )
          }
        }
      }).map(buildUpdate)
      //通过此项查看通过一系列转换后分区数据是否能对上

      dd.foreachPartition(ls => {
//        val pid = TaskContext.get().partitionId()
        //println(s"${pid} ${ls}")

        if(!ls.isEmpty){
          Try(updateBalance(ls)) match {
            case Failure(ex) => {
              LOG.error(s"更新指定公司科目余额表数据发生异常！\\r\\n ${ls.toSeq}")
              ex.printStackTrace()
            }
            case _ => None
          }
        }
      })
    }
  }

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("VoucherOperationAndInsertDb1")
      .master(ApplicationProperties.get("spark.master"))
      .config("spark.sql.caseSensitive", "false")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.streaming.stopGracefullyOnShutdown","true")
      .config("spark.debug.maxToStringFields","200")
      .config("spark.cleaner.ttl","2000")
      .config("spark.default.parallelism","8")
      .config("spark.sql.session.timeZone","Asia/Shanghai")
      .getOrCreate()

    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(1))
    val topics = Array("VoucherItems4Spark")

    val stream = KafkaUtils.createDirectStream[String, SubjectBalanceSlim](
      streamingContext,
      PreferConsistent,
      Subscribe[String, SubjectBalanceSlim](topics, kafkaParams)
    )

    implicit val formats = DefaultFormats + new DateToLongSerializer

    /**
      * 科目余额表空记录有两种情况，1、科目表没有该科目记录
      * 2、科目表指定属期没有记录
      *    a) 在记录前插入 要获取科目信息表，并插入，处理方式和科目表没有记录一致
      *    b) 在记录后插入 要补到最新记录，处理方式和最近属期没有记录一致
      */
    stream.foreachRDD(rdd=> {

      import sparkSession.implicits._

      if(!rdd.isEmpty()){
//        val emptyNum = BigDecimal(0)
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges


        /**
          * 1、将Rdd数据转换成可用sql查询的数据，对数据集分组并统计
          */
        val dstreamDF = rdd.map(m=>m.value()).toDF().groupBy("companyId","accountPeriod", "subjectCode")
          //.sum( "currentDebitAmount" , "currentDebitQty","currentDebitNocarryAmount", "currentCreditAmount", "currentCreditQty","currentCreditNocarryAmount")
            .agg( expr("sum(currentDebitAmount) as currentDebitAmount"),
                  expr("sum(currentDebitQty) as currentDebitQty"),
                  expr("sum(currentDebitNocarryAmount) as currentDebitNocarryAmount"),
                  expr("sum(currentCreditAmount) as currentCreditAmount"),
                  expr("sum(currentCreditQty) as currentCreditQty"),
                  expr("sum(currentCreditNocarryAmount) as currentCreditNocarryAmount"))

        //临时缓存
        dstreamDF.persist()

        /**
          * 当前属期，下一个属期
          */

        val queryMinAndMaxStr = rdd.map(m=>m.value()).map(m=> s"""(company_id = ${m.companyId}  and subject_code = '${m.subjectCode}')""")
          .reduce(_ + " or " +_)

        /**
          * 2、查询科目表指定公司，指定科目的最小，最大会计属期
          */
        val balanceDF = DATABASE_URL.map(url => getTableDF(TARGET_TABLE, url, sparkSession)).reduce(_.union(_))

        JMETA.foreach(p => {
          val balanceDF = getTableDF(TARGET_TABLE, p._2, sparkSession)
          val balanceFilterDF = balanceDF
            .where(queryMinAndMaxStr)
            .groupBy("company_id","subject_code")
            .agg(expr("min(account_period) as accountPeriodStart"),expr("max(account_period) as accountPeriodEnd"))
            .selectExpr("company_id as companyId", "subject_code as subjectCode", "accountPeriodStart", "accountPeriodEnd")
          /**
            * 3、将步骤1和步骤2结果并集
            */

          val crossDF = dstreamDF.join(balanceFilterDF, Seq("companyId", "subjectCode"), "left_outer")
          //          .repartition(8,$"companyId",$"subjectCode")
          /**
            * 4.1 匹配不上的记录,补到最新
            */

          //        val sqlConnectField2 = Seq("company_id", "subject_code")
          val infoDF = DATABASE_URL.map(url => getTableDF("cd_subject_info", url, sparkSession)).reduce(_.union(_))
          //        val pNum = rdd.getNumPartitions

          before(crossDF, infoDF, sparkSession)
          after(crossDF, balanceDF, sparkSession)
          updateR(dstreamDF, balanceDF, sparkSession)
        })

        DATABASE_URL.foreach(url=> {
          val balanceDF = getTableDF(TARGET_TABLE, url, sparkSession)
          val balanceFilterDF = balanceDF
            .where(queryMinAndMaxStr)
            .groupBy("company_id","subject_code")
            .agg(expr("min(account_period) as accountPeriodStart"),expr("max(account_period) as accountPeriodEnd"))
            .selectExpr("company_id as companyId", "subject_code as subjectCode", "accountPeriodStart", "accountPeriodEnd")
          /**
            * 3、将步骤1和步骤2结果并集
            */

          val crossDF = dstreamDF.join(balanceFilterDF, Seq("companyId", "subjectCode"), "left_outer")
          //          .repartition(8,$"companyId",$"subjectCode")
          /**
            * 4.1 匹配不上的记录,补到最新
            */

          //        val sqlConnectField2 = Seq("company_id", "subject_code")
          val infoDF = DATABASE_URL.map(url => getTableDF("cd_subject_info", url, sparkSession)).reduce(_.union(_))
          //        val pNum = rdd.getNumPartitions

          before(crossDF, infoDF, sparkSession)
          after(crossDF, balanceDF, sparkSession)
          updateR(dstreamDF, balanceDF, sparkSession)
        })


        //保存offset
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })

    //人工中断
    sys.ShutdownHookThread {
      println("Gracefully stopping Spark Streaming Application at "+ new java.util.Date())
      streamingContext.stop(true, true)
      sparkSession.stop()
      println("Application stopped at "+ new java.util.Date())
    }

    // 开启监听
    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop(true,  true);
    sparkSession.stop()
  }

  /**
    * `SEQUENCE_ID` bigint(20) NOT NULL COMMENT '序号',
    * `COMPANY_ID` int(11) NOT NULL COMMENT '公司ID',
    * `ACCOUNT_PERIOD` int(11) NOT NULL COMMENT '会计属期（格式：YYYYMM）',
    * `SUBJECT_ID` bigint(20) NOT NULL COMMENT '科目ID',
    * `SUBJECT_CODE` varchar(12) NOT NULL COMMENT '科目编码',
    * `SUBJECT_NAME` varchar(50) NOT NULL COMMENT '科目名称',
    * `SUBJECT_FULL_NAME` varchar(160) NOT NULL COMMENT '科目全称（科目递归路径）',
    * `SUBJECT_CATEGORY` tinyint(4) NOT NULL COMMENT '科目分类（1：资产、2：负债、3：权益、4：损益）',
    * `LENDING_DIRECTION` tinyint(4) DEFAULT NULL COMMENT '科目表的借贷方向（1：借、2：贷）',
    * `INITIAL_QTY` decimal(20,8) DEFAULT NULL COMMENT '期初数量',
    * `INITIAL_DEBIT_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '期初借方余额',
    * `INITIAL_CREDIT_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '期初贷方余额',
    * `CURRENT_DEBIT_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '本期借方金额',
    * `CURRENT_DEBIT_QTY` decimal(20,8) DEFAULT NULL COMMENT '本期借方数量',
    * `CURRENT_CREDIT_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '本期贷方金额',
    * `CURRENT_CREDIT_QTY` decimal(20,8) DEFAULT NULL COMMENT '本期贷方数量',
    * `CURRENT_DEBIT_NOCARRY_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '本期借方金额(不包括结转)',
    * `CURRENT_CREDIT_NOCARRY_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '本期贷方金额(不包括结转)',
    * `ENDING_DEBIT_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '期末借方余额',
    * `ENDING_CREDIT_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '期末贷方余额',
    * `ENDING_QTY` decimal(20,8) DEFAULT NULL COMMENT '期末数量',
    * `YEAR_DEBIT_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '本年累计借方金额',
    * `YEAR_CREDIT_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '本年累计贷方金额',
    * `YEAR_CREDIT_QTY` decimal(20,8) DEFAULT NULL COMMENT '本年累计贷方数量',
    * `YEAR_DEBIT_QTY` decimal(20,8) DEFAULT NULL COMMENT '本年累计借方数量',
    * `YEAR_DEBIT_NOCARRY_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '本年累计借方金额(不包括结转)',
    * `YEAR_CREDIT_NOCARRY_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '本年累计贷方金额(不包括结转)',
    * `SUBJECT_PARENT_CODE` varchar(20) NOT NULL COMMENT '上级科目',
    * `CREATE_TIME` datetime NOT NULL COMMENT '创建时间',
    *
    * @param s
    * @return
    */
  def buildInsertBalanceHead(s: SubjectInfoBrief, id:IdWorker, skipHead: Boolean = false): Seq[Array[Any]] ={
    //val debitAmount = if(s.lendingDirection == 1) s.initialAmount else BigDecimal(0)
    //val creditAmount = if(s.lendingDirection == 2) s.initialAmount else BigDecimal(0)
    val now = new java.util.Date()
    val javaBigZero = BigDecimal(0).bigDecimal
    val initQty = if( s.initialQty == null) javaBigZero else s.initialQty
    val (debitAmount, creditAmount, debitQty, creditQty) = if(s.lendingDirection == 1) (s.initialAmount.bigDecimal, javaBigZero , initQty, javaBigZero)
    else (javaBigZero, s.initialAmount.bigDecimal, javaBigZero, initQty)

    import com.spsoft.spark.hint.IntHints._
    //由于此处的结束属期是实际数据库存在的记录或未来一个月的属期，故用untilYm
    val months = s.accountPeriod.untilYm(s.accountPeriodEnd)
    for(month <- if (skipHead) months.tail else months) yield {
      Array[Any](id.nextId(), s.companyId, month, s.subjectId, s.subjectCode, s.subjectName, s.subjectFullName, s.subjectCategory //基本属性
        ,s.lendingDirection, initQty, debitAmount, creditAmount//期初
        ,0,0,0,0,0,0 //本期
        ,debitAmount, creditAmount, initQty //期末
        ,debitAmount, creditAmount, creditQty, debitQty, debitAmount, creditAmount //本年
        ,s.subjectParentCode, now
      )
    }
  }


  def nullReplaceZero(b: BigDecimal): BigDecimal  ={
    if(b == null) BigDecimal(0) else b
  }


  def buildInsertBalanceTail(s: SubjectBalance, id:IdWorker, skipHead: Boolean = false): Seq[Array[Any]] ={
    val now = new java.util.Date()
    val javaBigZero = BigDecimal(0).bigDecimal
    val initQty = if( s.endingQty == null) javaBigZero else s.endingQty //初始数量取期末
    val (debitAmount, creditAmount) = (s.endingDebitAmount, s.endingCreditAmount)
    val (yearDebitAmount, yearDebitQty, yearDebitNocarryAmount, yearCreditAmount, yearCreditQty, yearCreditNocarryAmount)  = (
      nullReplaceZero(s.yearDebitNocarryAmount) ,
      nullReplaceZero(s.yearCreditAmount) ,
      nullReplaceZero(s.yearCreditQty),
      nullReplaceZero(s.yearCreditAmount ),
      nullReplaceZero(s.yearCreditQty),
      nullReplaceZero(s.yearCreditNocarryAmount))


    import com.spsoft.spark.hint.IntHints._
    val months = s.accountPeriod.toYm()
    for(month <- if (skipHead) months.tail else months) yield {
      Array[Any](id.nextId(), s.companyId, month, s.subjectId, s.subjectCode, s.subjectName, s.subjectFullName, s.subjectCategory //基本属性
        ,s.lendingDirection, initQty, debitAmount, creditAmount//期初，取上个月期末
        ,0,0,0,0,0,0 //本期
        ,debitAmount, creditAmount, initQty //期末，取上个月期末
        ,yearDebitAmount, yearCreditAmount, yearCreditQty, yearDebitQty, yearDebitNocarryAmount, yearCreditNocarryAmount //本年，取上个月本年
        ,s.subjectParentCode, now
      )
    }
  }

  /**
    * """ UPDATE AT_SUBJECT_BALANCE SET
    * |INITIAL_DEBIT_AMOUNT = INITIAL_DEBIT_AMOUNT + %f,
    * |INITIAL_CREDIT_AMOUNT = INITIAL_CREDIT_AMOUNT + %f,
    * |INITIAL_QTY = INITIAL_QTY + %f,
    * |CURRENT_DEBIT_AMOUNT = CURRENT_DEBIT_AMOUNT + %f,
    * |CURRENT_DEBIT_QTY = CURRENT_DEBIT_QTY + %f,
    * |CURRENT_DEBIT_NOCARRY_AMOUNT = CURRENT_DEBIT_NOCARRY_AMOUNT + %f,
    * |CURRENT_CREDIT_AMOUNT = INITIAL_CREDIT_AMOUNT + %f,
    * |CURRENT_CREDIT_QTY = CURRENT_CREDIT_QTY + %f,
    * |CURRENT_CREDIT_NOCARRY_AMOUNT = CURRENT_CREDIT_NOCARRY_AMOUNT + %f,
    * |ENDING_DEBIT_AMOUNT = ENDING_DEBIT_AMOUNT + %f,
    * |ENDING_CREDIT_AMOUNT = ENDING_CREDIT_AMOUNT + %f,
    * |ENDING_QTY = ENDING_QTY + %f,
    * |YEAR_DEBIT_AMOUNT = YEAR_DEBIT_AMOUNT + %f,
    * |YEAR_DEBIT_QTY = YEAR_DEBIT_QTY + %f,
    * |YEAR_DEBIT_NOCARRY_AMOUNT = YEAR_DEBIT_NOCARRY_AMOUNT + %f,
    * |YEAR_CREDIT_AMOUNT = YEAR_CREDIT_AMOUNT + %f,
    * |YEAR_CREDIT_QTY = YEAR_CREDIT_QTY + %f,
    * |YEAR_CREDIT_NOCARRY_AMOUNT = YEAR_CREDIT_NOCARRY_AMOUNT + %f
    * |WHERE COMPANY_ID = %d AND SUBJECT_CODE = '%s' AND ACCOUNT_PERIOD = %d
    * |""".stripMargin
    *
    * @param s
    * @return
    */
  def buildUpdate(s: SubjectBalanceMedium): Array[Any] = {
    val now = new java.util.Date()
    Array[Any](s.initialDebitAmount, s.initialCreditAmount, s.initialQty, //期初
      s.currentDebitAmount, s.currentDebitQty, s.currentDebitNocarryAmount, s.currentCreditAmount, s.currentCreditQty, s.currentCreditQty, //本期
      s.endingDebitAmount, s.endingCreditAmount, s.endingQty, //期末
      s.yearDebitAmount, s.yearDebitQty, s.yearDebitNocarryAmount, s.yearCreditAmount, s.yearCreditQty, s.yearCreditNocarryAmount, //本年
      s.companyId, s.subjectCode, s.accountPeriod //条件信息
    )
  }


  /**
    * @param s
    * @return
    */
  def updateBalance(data: Iterator[Array[Any]]): Unit ={
    if(!data.isEmpty){
      val update =
        s""" UPDATE ${TARGET_TABLE} SET
          |INITIAL_DEBIT_AMOUNT = INITIAL_DEBIT_AMOUNT + %f,
          |INITIAL_CREDIT_AMOUNT = INITIAL_CREDIT_AMOUNT + %f,
          |INITIAL_QTY = INITIAL_QTY + %f,
          |CURRENT_DEBIT_AMOUNT = CURRENT_DEBIT_AMOUNT + %f,
          |CURRENT_DEBIT_QTY = CURRENT_DEBIT_QTY + %f,
          |CURRENT_DEBIT_NOCARRY_AMOUNT = CURRENT_DEBIT_NOCARRY_AMOUNT + %f,
          |CURRENT_CREDIT_AMOUNT = INITIAL_CREDIT_AMOUNT + %f,
          |CURRENT_CREDIT_QTY = CURRENT_CREDIT_QTY + %f,
          |CURRENT_CREDIT_NOCARRY_AMOUNT = CURRENT_CREDIT_NOCARRY_AMOUNT + %f,
          |ENDING_DEBIT_AMOUNT = ENDING_DEBIT_AMOUNT + %f,
          |ENDING_CREDIT_AMOUNT = ENDING_CREDIT_AMOUNT + %f,
          |ENDING_QTY = ENDING_QTY + %f,
          |YEAR_DEBIT_AMOUNT = YEAR_DEBIT_AMOUNT + %f,
          |YEAR_DEBIT_QTY = YEAR_DEBIT_QTY + %f,
          |YEAR_DEBIT_NOCARRY_AMOUNT = YEAR_DEBIT_NOCARRY_AMOUNT + %f,
          |YEAR_CREDIT_AMOUNT = YEAR_CREDIT_AMOUNT + %f,
          |YEAR_CREDIT_QTY = YEAR_CREDIT_QTY + %f,
          |YEAR_CREDIT_NOCARRY_AMOUNT = YEAR_CREDIT_NOCARRY_AMOUNT + %f
          |WHERE COMPANY_ID = %d AND SUBJECT_CODE = '%s' AND ACCOUNT_PERIOD = %d
          |""".stripMargin.replaceAll("\n", " ")
      val sqlArray = for(d <- data) yield {
        update.format(d: _*)
      }
//      TODO
//      DataSourcePoolUtils.executeBatch(sqlArray)
  }

  }

  /**
    * 插入初始记录
    * @param data
    * @return
    */
  def insetBalanceInitial(data: Iterator[Array[Any]]) = {
    if(!data.isEmpty){
      val executeSQL = s"""INSERT INTO ${TARGET_TABLE}(SEQUENCE_ID, COMPANY_ID, ACCOUNT_PERIOD,
                         |SUBJECT_ID, SUBJECT_CODE, SUBJECT_NAME, SUBJECT_FULL_NAME, SUBJECT_CATEGORY, LENDING_DIRECTION,
                         |INITIAL_QTY, INITIAL_DEBIT_AMOUNT, INITIAL_CREDIT_AMOUNT,
                         |CURRENT_DEBIT_AMOUNT, CURRENT_DEBIT_QTY, CURRENT_CREDIT_AMOUNT, CURRENT_CREDIT_QTY, CURRENT_DEBIT_NOCARRY_AMOUNT, CURRENT_CREDIT_NOCARRY_AMOUNT,
                         |ENDING_DEBIT_AMOUNT, ENDING_CREDIT_AMOUNT, ENDING_QTY,
                         |YEAR_DEBIT_AMOUNT, YEAR_CREDIT_AMOUNT,YEAR_CREDIT_QTY,YEAR_DEBIT_QTY, YEAR_DEBIT_NOCARRY_AMOUNT, YEAR_CREDIT_NOCARRY_AMOUNT,
                         |SUBJECT_PARENT_CODE, CREATE_TIME,  NEXT_PERIOD)
                         |VALUES(?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?, 999999)""".stripMargin.replaceAll("\n", StringUtils.EMPTY)
//      data.foreach(a=> println(a.toSeq))
      //TODO
//      DataSourcePoolUtils.executeBatch(executeSQL, data)
    }
  }
}
