package com.spsoft.spark.utils

import java.sql.{Connection, SQLException}

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource
import org.apache.commons.lang3.StringUtils
import org.apache.spark.TaskContext
import org.slf4j.LoggerFactory

/**
  * Mysql 连接池
  */
object CloseableMysqlUtils {
  private val LOG = LoggerFactory.getLogger(CloseableMysqlUtils.getClass)

  val dataSource: Option[DataSource] = {
    try {
      Some(DruidDataSourceFactory.createDataSource(DataSourceProperties.properties))
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

  def close()(implicit conn:Option[Connection]) = {
    conn match {
      case Some(ds) => conn.get.close()
      case None => None
    }
  }

  def executeBatch(string: String, data: Iterator[Array[Any]]) = {
    require(StringUtils.isNoneBlank(string),"prepared execute string cannot be null or empty")
    require(!data.isEmpty,"prepared execute data cannot be empty")
    implicit val conn = getConnection
    if(conn.isEmpty){
      throw new IllegalAccessException("无法获取数据库连接...")
    }
    try{
      conn.get.setAutoCommit(false)
      val prepare = conn.get.prepareStatement(string)
      data.foreach(row=> {
        for(i <- 1 to row.length){
          row(i-1) match {
            case x: BigDecimal => prepare.setObject(i, x.bigDecimal)
            case _ => prepare.setObject(i, row(i-1))
          }
        }
        prepare.addBatch()
      })
      val result = prepare.executeBatch()
      conn.get.commit()
      conn.get.setAutoCommit(true)
      println(conn.get+ " insert "+ TaskContext.get.partitionId()+ " "+result.toSeq + " "+ result.length + " "+Thread.currentThread().getId)
    }catch {
      case e: SQLException =>
        println("sss")
    }finally {
      close()
    }
  }

  def executeBatch(sqlArray: Iterator[String]) = {
    require(!sqlArray.isEmpty,"prepared execute sql array cannot be empty")
    implicit val conn = getConnection
    if(conn.isEmpty){
      throw new IllegalAccessException("无法获取数据库连接...")
    }
    try{
      conn.get.setAutoCommit(false)
      val stmt = conn.get.createStatement()
      sqlArray.foreach(stmt.addBatch)
      val result = stmt.executeBatch()
      conn.get.commit()
      conn.get.setAutoCommit(true)
      println(conn.get+ " insert "+ TaskContext.get.partitionId()+ " "+result.toSeq + " "+ result.length + " "+Thread.currentThread().getId)
    }catch {
      case e: SQLException =>
        println("sss")
    }finally {
      close()
    }
  }

  def main(args: Array[String]): Unit = {
    println(dataSource.get.getConnection)
    executeBatch("aaa", Iterator())
  }
}


