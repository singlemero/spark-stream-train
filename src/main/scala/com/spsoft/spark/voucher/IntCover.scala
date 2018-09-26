package com.spsoft.spark.voucher

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar

object IntCover {

  implicit class Improvement(val num : Int){   //隐式类

    private val sdf8 = new SimpleDateFormat("yyyyMMdd")

    private val sdf6 = new SimpleDateFormat("yyyyMM")

    private val sdf4 = new SimpleDateFormat("yyyy")

    protected def toDate: java.util.Date = {
      num.toString.length match {
        case  8 => sdf8.parse(num.toString)
        case  6 => sdf6.parse(num.toString)
        case  4 => sdf4.parse(num.toString)
      }
    }

    def between(d: Date = new Date(System.currentTimeMillis())) = {

      val func = (tmp:java.util.Date)=> {
        val c1 = Calendar.getInstance
        val c2 = Calendar.getInstance
        c2.setTimeInMillis(d.getTime)
        c1.setTime(tmp)
        (c2.get(Calendar.YEAR) - c1.get(Calendar.YEAR) ) * 12 + c2.get(Calendar.MONTH) - c1.get(Calendar.MONTH)
      }
      func(toDate)
    }

    def months(d: Date = new Date(System.currentTimeMillis())):Seq[Int] = {
      val months = between(d)
      val c1 = Calendar.getInstance
      c1.setTime(toDate)
      c1.add(Calendar.MONTH, -1)
      for(i <-0 until months) yield {
          c1.add(Calendar.MONTH, 1)
          sdf6.format(c1.getTime).toInt
      }
    }

    def months(d: Int):Seq[Int] = {
      val months = between(new Date(d.toDate.getTime))
      val c1 = Calendar.getInstance
      c1.setTime(toDate)
      c1.add(Calendar.MONTH, -1)
      for(i <-0 until months) yield {
        c1.add(Calendar.MONTH, 1)
        sdf6.format(c1.getTime).toInt
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val i = 201803
    println(i.months(201805))
    println(i.months())
  }
}
