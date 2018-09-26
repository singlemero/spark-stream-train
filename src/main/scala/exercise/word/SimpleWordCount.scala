package exercise.word

import exercise.sql.{Person, ZClass}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer
import util.control.Breaks._
import java.util

object SimpleWordCount {

  def main(args: Array[String]): Unit = {

    //val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(1))

    //import ssc.sparkContext.implicits._
    val str = """"""
    val sb = new StringBuilder()
    var up = false
    for(i <- str.toLowerCase){
      breakable{
        if(up){
          up = false
          sb.append(i.toUpper)
          break()
        }

        if(i.equals('_')){
          up = true
        }

        if(!up){
          sb.append(i)
        }
      }
    }
    printf(sb.toString())


    val c = Seq(ZClass(1, "语文", 1), ZClass(2, "数学", 1),  ZClass(2, "英语", 1), ZClass(3, "政治", 1), ZClass(4, "生物", 2), ZClass(5, "物理", 2))
    val ds = ssc.sparkContext.parallelize(c)
    ds.map(m=> (m.pId, m)).groupByKey().map(m=> {

      val name = m._2.map(_.name).reduce(_.concat(_))
      val newid = m._2.map(_.id).reduce(_+_)
      ZClass(newid, name, m._1)
    }).foreach(nz => printf(nz.toString))

    printf((20180131/100).toString)

    var list = List[Int]()
    list = list :+ 1
    //list = list :+ 2
    print(list)


    val l = List("1"->List(1,2), "2"-> List(2,3), "3"->List(3,4))
    l.map(k=> {
      var l = 0;
      sparkSession.sqlContext
      //ssc.sparkContext.parallelize(k._2).toJavaRDD().toDf
      (k._1, l)
    }).foreach(a=> println(a._2))
//    ds

    for(i<-0 to 0){
      println("dfasfasdfdsfasf")
    }

    var list1 = List[String]()
    list1 = list1 :+ "aaa"
    list1 = list1 :+ "bbb"
    import scala.collection.JavaConverters._
    val x:java.util.List[String] = list1.asJava
    println(org.apache.commons.lang3.StringUtils.join(x,","))


    val ll = 10L;
    print(ll%3)
  }

  def sparkSession: SparkSession = {
    SparkSession.builder().appName("JdbcVoucherOperation").master("local[4]").getOrCreate()
  }
}
