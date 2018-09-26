package com.spsoft.spark.voucher.util

import java.sql.Connection
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource
import org.slf4j.{Logger, LoggerFactory}

object MysqlPoolUtils {
  private val LOG = LoggerFactory.getLogger(MysqlPoolUtils.getClass)

  val dataSource: Option[DataSource] = {
    try {
      val druidProps = new Properties()
      // 获取Druid连接池的配置文件
      val druidConfig = getClass.getResourceAsStream("/fw-datasoure-taxdb.properties")
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
    val c = getConnection.get.prepareStatement("")
    //c.setObject()
    for(i <- 0 to 10){
      println(getConnection.get)
    }
  }
}


