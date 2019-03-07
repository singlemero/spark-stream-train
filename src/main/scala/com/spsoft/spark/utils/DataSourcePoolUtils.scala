package com.spsoft.spark.utils

import com.spsoft.spark.utils.EnhanceUtils.autoClose
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.TaskContext
import org.slf4j.LoggerFactory
import scala.util.{Failure, Success, Try}

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

  lazy val dataSource = {
    Try(new HikariDataSource(config)).transform(s=> Success(s),e=>{
      LOG.error("Error Create Mysql Connection", e)
      Failure(e)
    }).get
  }

  def executeBatch(string: String, data: Iterator[Array[Any]]) = {
    require(StringUtils.isNoneBlank(string),"prepared execute string cannot be null or empty")
    require(!data.isEmpty,"prepared execute data cannot be empty")
    autoClose(dataSource.getConnection) { conn =>
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
//      LOG.warn("insert " + TaskContext.get.partitionId() + " " + result.toSeq + " " + result.length)
      LOG.warn(s"partitionId:${TaskContext.get.partitionId()} insert ${result.length}")
    }
  }

  def executeBatch(sqlArray: Iterator[String]) = {
    require(!sqlArray.isEmpty,"prepared execute sql array cannot be empty")
    autoClose(dataSource.getConnection) { conn =>
      conn.setAutoCommit(false)
      val stmt = conn.createStatement()
      //println(sqlArray.isEmpty)
      sqlArray.foreach(stmt.addBatch)
      val result = stmt.executeBatch()
      conn.commit()
      LOG.warn(s"partitionId:${TaskContext.get.partitionId()} update ${result.length}")
    }
  }

  def main(args: Array[String]): Unit = {
    val num = 100;
//    for (i <- 0 to 100) {
//      new Thread(new Runnable {
//        override def run(): Unit = {
//          val connection = dataSource.getConnection
//          println(Thread.currentThread().getName + "  " + connection)
////          connection.close()
//        }
//      }).start()
//    }

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
