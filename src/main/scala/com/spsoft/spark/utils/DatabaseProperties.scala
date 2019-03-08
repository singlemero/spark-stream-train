package com.spsoft.spark.utils
import java.util
import java.util.function.BiConsumer
import java.util.stream.Collectors

import com.spsoft.DataMeta

import scala.collection.JavaConverters._
import com.spsoft.spark.common.ProjectConstants._
/**
  *
  * @auther: lhkong
  * @date: 2019/3/7 14:04
  */
object DatabaseProperties extends PropertiesLoader{
  override def inputStream: String = DATABASE_PROPERTIES_PATH1


  lazy val meta = {
    properties.asScala.map(
      {m=>(m._1.replace("database.",""),m._2)}
//      (_._1,_._2)
    ).groupBy(
//      {p=>
//      p._1.split("\\.")(0)}
//      p=>p._1.split("")(0)
      _._1.split("\\.")(0).toLong
    ).map(
//      {v=>{
//      val c = new DataMeta
//      v._2.foreach({o => o._1 match {
//        case x if x.contains("url") => c.setUrl(o._2)
//        case x if x.contains("user") => c.setUser(o._2)
//        case x if x.contains("password") => c.setPassword(o._2)
//      }})
//      (v._1,c)
//    }}
      {p=>
        (p._1, BriefMeta(
          p._2.find(m=>m._1.contains("url")).get._2,
          p._2.find(m=>m._1.contains("user")).get._2,
          p._2.find(m=>m._1.contains("password")).get._2
        ))
      }
    )
  }
  def main(args: Array[String]): Unit = {
    println(get("database"))
    val a = (b:String)=> b.replace("database.","")
    val c = (b:String)=> b
    meta.foreach((x)=>{
      print(x)
    })
//    new Function[String]{}
    properties
//    properties.stringPropertyNames().asScala.collect()
//    properties.stringPropertyNames().stream().collect(Collectors.toMap(new Function[String](){}, c))
  }
}


