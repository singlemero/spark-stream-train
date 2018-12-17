package com.spsoft.spark.utils

import java.sql.Date

import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

object EnhanceUtils {

  /**
    *
    * <: 边界限定
    * def  方法名[参数约定或返回值约定] (接受参数) (匿名函数，定义函数接受与返回值)()...
    *
    *
    *
    * @param closeable
    * @param fun
    * @tparam A
    * @tparam B
    * @return
    */
  def autoClose[A <: AutoCloseable,B](closeable: A)(fun: (A) => B): B = {
    tryAutoClose(closeable)(fun).get
  }

  private def tryAutoClose[A <: AutoCloseable,B](closeable: A)(fun: (A) => B): Try[B] = {

    Try(fun(closeable)).transform(
      result => {
        closeable.close()
        Success(result)
      },
      e => {
        Try(closeable.close()).transform(
          _ => Failure(e),
          closeEx ⇒ {
            e.addSuppressed(closeEx)
            Failure(e)
          }
        )
      }
    )
  }


  def switchCond[A,R](fun:(R) => A,cond1:() => Option[R], cond2:() =>Option[R]):A={
     fun(cond1().orElse(cond2()).get)
  }

  def switchCond1[A,B,R](fun:(R) => A,p:B)(cond1:(B)=>Option[R])(cond2:(B)=>Option[R]):A={
    fun(cond1(p).orElse(cond2(p)).get)
  }

  def crop[A,B,R](a:A)(fun:(A)=>A) = {
    fun(a)
  }

  def cc[B](a:DataFrame,b: B)(fun:(DataFrame)=>B) = {

  }

  def crop(d: java.sql.Date) = {
    import com.spsoft.spark.hint.DateHints._
    print(d.nextMonth())
//    Some(a).getOrElse(BigDecimal(0))
  }

  def main(args: Array[String]): Unit = {
    val f = (s:String) => s.length
    val c1 = () => None
    val c2 = () => Some("this gg")
    //println(switchCond(f, c1, c2))
    //println(switchCond1(f, "dsfdasfadsfadsfdsafds"){c1=> None}{c2=> Some(c2)})




    crop(new Date(System.currentTimeMillis()))
  }
}
