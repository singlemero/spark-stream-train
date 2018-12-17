package com.spsoft.spark.utils

import java.sql.{Connection, SQLException}

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import javax.sql.DataSource
import org.apache.commons.lang3.StringUtils
import org.apache.spark.TaskContext
import org.slf4j.LoggerFactory

object DataSourcePoolUtils {

  private val LOG = LoggerFactory.getLogger(DataSourcePoolUtils.getClass)

  lazy val config: HikariConfig = {

    val config = new HikariConfig
    print(DataSourceProperties.get("url"))
    config.setJdbcUrl(DataSourceProperties.get("url"))
    config.setUsername(DataSourceProperties.get("username"))
    config.setPassword(DataSourceProperties.get("password"))
    config.addDataSourceProperty("cachePrepStmts", "true")
    config.addDataSourceProperty("prepStmtCacheSize", "250")
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    config.addDataSourceProperty("maximumPoolSize", "20")
    config.setConnectionInitSql("select 1")
    config.setConnectionTestQuery("select 1")
    config.addHealthCheckProperty("connectivityCheckTimeoutMs", "1000")
//    config.setValidationTimeout()
//    config.addHealthCheckProperty()
    config
  }

  lazy val dataSource: Option[DataSource] = {
    try {
      //LOG.warn(TaskContext.get.partitionId)
      val t = TaskContext.get
      val id = if (t == null) Thread.currentThread().getName else t.partitionId()
      LOG.warn(s"${id} initial dataSource")
      Some(new HikariDataSource(config))
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

  def close()(implicit conn:Connection) = {
    if(conn != null && !conn.isClosed){
      conn.setAutoCommit(true)
      conn.close()
    }
  }

  def executeBatch(string: String, data: Iterator[Array[Any]]) = {
    require(StringUtils.isNoneBlank(string),"prepared execute string cannot be null or empty")
    require(!data.isEmpty,"prepared execute data cannot be empty")
    implicit var conn: Connection = null
    try{
      conn = getConnection.get
      conn.setAutoCommit(false)
      val prepare = conn.prepareStatement(string)
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
      conn.commit()
      LOG.warn(conn+ " insert "+ TaskContext.get.partitionId()+ " "+result.toSeq + " "+ result.length + " "+Thread.currentThread().getId)
    }catch {
      case e: SQLException =>
        e.printStackTrace()
    }finally {
      close()
    }
  }

  def executeBatch(sqlArray: Iterator[String]) = {
    require(!sqlArray.isEmpty,"prepared execute sql array cannot be empty")
    implicit var conn: Connection = null
    try{
      conn = getConnection.get
      conn.setAutoCommit(false)
      val stmt = conn.createStatement()
      //println(sqlArray.isEmpty)
      sqlArray.foreach(stmt.addBatch)
      val result = stmt.executeBatch()
      conn.commit()
      LOG.warn(conn+ " update "+ TaskContext.get.partitionId()+ " "+result.toSeq + " "+ result.length + " "+Thread.currentThread().getId)
    }catch {
      case e: SQLException =>
        e.printStackTrace()
    }finally {
      close()
    }
  }

  def main(args: Array[String]): Unit = {
    val num = 100;
    for (i <- 0 to 100) {
      new Thread(new Runnable {
        override def run(): Unit = {
          val connection = dataSource.get.getConnection
          println(Thread.currentThread().getName + "  " + connection)
          connection.close()
        }
      }).start()
    }
  }
}
