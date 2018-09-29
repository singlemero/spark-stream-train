package com.spsoft.spark.utils

import java.sql.Connection
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource
import com.spsoft.spark.voucher.common.ProjectConstants._
import org.slf4j.{Logger, LoggerFactory}

/**
  * Mysql 连接池
  */
object MysqlPoolUtils {
  private val LOG = LoggerFactory.getLogger(MysqlPoolUtils.getClass)

  val dataSource: Option[DataSource] = {
    try {
      val druidProps = new Properties()
      // 获取Druid连接池的配置文件
      val druidConfig = getClass.getResourceAsStream(DATABASE_PROPERTIES_PATH)
      // 倒入配置文件
      druidProps.load(druidConfig)
      Some(DruidDataSourceFactory.createDataSource(druidProps))
    } catch {
      case error: Exception =>
        LOG.error("Error Create Mysql Connection", error)
        None
    }
  }

  // 连接方式
  def getConnection: Option[Connection] = {
    dataSource match {
      case Some(ds) => Some(ds.getConnection())
      case None => None
    }
  }

  def closeConnection(conn:Option[Connection]) = {
    conn match {
      case Some(ds) => conn.get.close()
      case None => None
    }
  }
  def closeC()(implicit conn:Option[Connection]) = {
    conn match {
      case Some(ds) => conn.get.close()
      case None => None
    }
  }

  def main(args: Array[String]): Unit = {
    val l = getClass.getResourceAsStream(DATABASE_PROPERTIES_PATH)
    val p = new Properties()
    p.load(l)
    println(p)
    println(p.propertyNames())

  }
}


