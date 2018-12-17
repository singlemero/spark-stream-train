package exercise.basic

import com.spsoft.common.utils.IdWorker
import com.spsoft.spark.utils.CloseableMysqlUtils.dataSource
import com.spsoft.spark.voucher.vo.SubjectBalance
import exercise.sql.ZZ
import org.apache.commons.lang.math.RandomUtils
import org.apache.kafka.common.serialization.StringDeserializer

import scala.reflect.runtime.universe._
object Practice {



  def partFun ={
    val fun2 = (content:String) =>  println(content)
    fun2("aaa")

    def func_return(content: String) = (message: String)=> println(message)
    func_return("tttt")("fdffdf")


    def sum(a: Int) = (b:Int) => a+b


    def ace(func: (String) => Unit, name: String) = {}

    println(sum(10)(5))
  }

  def caseClass = {
    val a: List[Any] = List("aaaa",789, ZZ(1,1,"ZZZ"))

    a.foreach({
      case x:Int => println(s"this is $x")
      case x:String => println(x)
      case x:ZZ => println(x)
    })
  }

  def testHashCode = {
    val a1 = "3591201801312203"
    val a2 = "35912018013122030200"

    println(a1.hashCode%3)
    println(a2.hashCode%3)
  }

  def testIntCover = {
    import com.spsoft.spark.hint.IntHints._
    val num  = 201706
    println(num.calculate())
    println(num.untilYm().length)
    for(i <- num.untilYm()){
      println(i)
    }
  }

  def testSnow = {
    val a = ZZ(33, BigDecimal(8),"lili")
    val rm = scala.reflect.runtime.currentMirror
    val accessors = rm.classSymbol(a.getClass).toType.members.collect {
      case m: MethodSymbol if m.isGetter && m.isPublic => m
    }
    val instanceMirror = rm.reflect(a)
    for(acc <- accessors)
      println(s"$a: ${instanceMirror.reflectMethod(acc).apply()}")
  }

  def testReflection = {
    val a = ZZ(33, BigDecimal(8),"lili")
    val rm = scala.reflect.runtime.currentMirror
    //获取属姓名
    val accessors = rm.classSymbol(classOf[ZZ]).toType.members.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
      //case m: MethodSymbol if m.isGetter && m.isPublic => m
    }
    val instanceMirror = rm.reflect(a)

    //rm.classSymbol(classOf[ZZ]).toType.
    rm.classSymbol(classOf[ZZ]).toType.members.foreach(f=>{
      //println(f)
    })

    for(acc <- accessors){
      val c = instanceMirror.reflectField(acc)
      //println(c.symbol.name)
      //println(c.symbol)
      //println(acc)//属性名
      //println(s"$a: ${instanceMirror.reflectMethod(acc).apply()}")
    }

    val vals = typeOf[ZZ].members.collect{
      case t: TermSymbol if t.isVal=> t
    }

    val vars = typeOf[ZZ].members.filter( _ match{
      case t : TermSymbol => t.isVal
      case _ => false
    })

    //rm.reflect(vars)
    //vals

    vals.foreach( v => v match{
      case t : TermSymbol => {

            t.getter.asMethod.returnType match{
              case t if t =:= typeOf[String] => {
                println("string")
              }
              // Primitive型は、JavaUniverse.definitionsに定義されている
              case definitions.IntTpe => {
                println("int")
                //setterMethod(v.toInt)
              }
              case definitions.BooleanTpe => {
                println("BooleanTpe")
              }
              case t if t =:= typeOf[BigDecimal] =>{
                println("big")
              }
              case t => println("Unknown type " + t)
            }
          }
      }
    )

    vals.foreach(v=> v.getter.asMethod.returnType match {
      case t if t =:= typeOf[String] => println("gggggggg")
      case t if t =:= typeOf[String] => println("gggggggg")
      case _ => ;
    })
  }

  def testBit = {
    println (255<<7)

    println(24704 >> 7)
    println(24704 & 127)
    //println(24704 & )
  }

  def testIdWork = {
    val id0 = IdWorker.getInstance("SUBJECT_BALANCE",0)
    val id5 = IdWorker.getInstance("SUBJECT_BALANCE",5)
    val id7 = IdWorker.getInstance("SUBJECT_BALANCE",7)
    val num = 100000;
    Seq(id0,id5,id7).flatMap(f=>{
      for(i <- 0 until num)yield {
        (f.nextId(), 1)
      }
    }).groupBy(_._1).map(m=> (m._1,m._2.map(_._2).reduce(_+_))).filter(_._2>1).foreach(println)
    println("end")
  }

  def testRandom = {
    val c = for(i <-0 to 100) yield{
      (RandomUtils.nextInt(20).toString,1)
    }
    c.groupBy(_._1).map(m=> (m._1,m._2.map(_._2).reduce(_+_))).foreach(println)

  }

  def testKafkas = {
    println(classOf[StringDeserializer])
  }

  def testCaseDef = {
    val z = new ZZ(7,null, "gggg")
    println(z)
  }

  def testAlg = {
    println(Array(1,1,2,2,3,3,4,5,5).reduceLeft(_^_))
  }

  def testHikariCp = {
    import com.zaxxer.hikari.HikariDataSource
    import com.zaxxer.hikari.HikariConfig
    import com.zaxxer.hikari.HikariDataSource
    val config = new HikariConfig
    config.setJdbcUrl("jdbc:mysql://192.168.55.215:8066/qf_accdb?characterEncoding=utf8&useSSL=false")
    config.setUsername("qf_user1")
    config.setPassword("hwsofti201710")
    config.addDataSourceProperty("cachePrepStmts", "true")
    config.addDataSourceProperty("prepStmtCacheSize", "250")
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    config.addDataSourceProperty("maximumPoolSize", "20")
    val ds = new HikariDataSource(config)
    println(ds)
    val num = 100;
    for(i <- 0 to num){
      new Thread(new Runnable {
        override def run(): Unit = {
          val connection = ds.getConnection
          println(Thread.currentThread().getName + "  " +connection)
          //connection.close()
        }
      }).start()
    }
  }

  def main(args: Array[String]): Unit = {
    //caseClass
    //testHashCode
    //testIntCover
    //testReflection
    //testAlg
    //println(7^9)
    //println(0xb111)
    val i:BigInt = 1538236800000L
    println(i)
    testHikariCp
  }


}
