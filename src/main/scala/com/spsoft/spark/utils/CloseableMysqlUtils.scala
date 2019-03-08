package com.spsoft.spark.utils

import com.spsoft.spark.utils.EnhanceUtils._
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import javax.sql.DataSource
import org.apache.commons.lang3.StringUtils
import org.apache.spark.TaskContext
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Mysql 连接池
  */
object CloseableMysqlUtils {
  private val LOG = LoggerFactory.getLogger(CloseableMysqlUtils.getClass)

  lazy val dataSourceMap = scala.collection.mutable.Map[BriefMeta, DataSource]()


  def getDS(briefMeta: BriefMeta) ={
    dataSourceMap.get(briefMeta).getOrElse(build(briefMeta))
//    Option(dataSourceMap(briefMeta)) match {
//      case Some(x) => x
//      case None => build(briefMeta)
//    }
  }

  def build(briefMeta: BriefMeta) = {
    LOG.info(s"DataSource ${briefMeta.url}")
    val config = new HikariConfig
    config.setJdbcUrl(briefMeta.url)
    config.setUsername(briefMeta.user)
    config.setPassword(briefMeta.password)
    config.addDataSourceProperty("cachePrepStmts", "true")
    config.addDataSourceProperty("prepStmtCacheSize", "250")
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    config.addDataSourceProperty("maximumPoolSize", "20")
    config.setConnectionInitSql("select 1")
    config.setConnectionTestQuery("select 1")
    config.addHealthCheckProperty("connectivityCheckTimeoutMs", "1000")
    //    config.setValidationTimeout()
    //    config.addHealthCheckProperty()
    val rs = Try(new HikariDataSource(config)).transform(s=> Success(s),e=>{
      LOG.error("Error Create Mysql Connection", e)
      Failure(e)
    }).get
    dataSourceMap+=(briefMeta -> rs)
    rs
  }

  lazy val config: HikariConfig = {
    LOG.info(s"DataSource ${DataSourceProperties.get("url")}")
    val config = new HikariConfig
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

  lazy val dataSource = {
    Try(new HikariDataSource(config)).transform(s=> Success(s),e=>{
      LOG.error("Error Create Mysql Connection", e)
      Failure(e)
    }).get
  }




  def executeBatch(string: String, data: Iterator[Array[Any]])(implicit db: BriefMeta) = {
    require(StringUtils.isNoneBlank(string),"prepared execute string cannot be null or empty")
    require(!data.isEmpty,"prepared execute data cannot be empty")
    autoClose(getDS(db).getConnection) { conn =>
      conn.setAutoCommit(false)
      val prepare = conn.prepareStatement(string)
      data.foreach(row => {
        for (i <- 1 to row.length) {
          row(i - 1) match {
            case x: BigDecimal => prepare.setObject(i, x.bigDecimal)
            case _ => prepare.setObject(i, row(i - 1))
          }
        }
        prepare.addBatch()
      })
      val result = prepare.executeBatch()
      conn.commit()
      println(conn + " insert " + TaskContext.get.partitionId() + " " + result.toSeq + " " + result.length + " " + Thread.currentThread().getId)
    }
  }

  def executeBatch(sqlArray: Iterator[String])(implicit db: BriefMeta) = {
    require(!sqlArray.isEmpty,"prepared execute sql array cannot be empty")
    autoClose(getDS(db).getConnection) { conn =>
      conn.setAutoCommit(false)
      val stmt = conn.createStatement()
      //println(sqlArray.isEmpty)
      sqlArray.foreach(stmt.addBatch)
      val result = stmt.executeBatch()
      conn.commit()
      println(conn+ " update "+ TaskContext.get.partitionId()+ " "+result.toSeq + " "+ result.length + " "+Thread.currentThread().getId)
    }
  }

  def main(args: Array[String]): Unit = {
    val num = 10;
    for(i <- 0 to num){
      new Thread(new Runnable {
        override def run(): Unit = {
          autoClose(dataSource.getConnection){conn=>
            println(Thread.currentThread().getName + "  " +conn)
          }
        }
      }).start()
    }
  }
}


