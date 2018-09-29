package com.spsoft.spark.hint

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar

object IntHints {

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

    def calculate(d: Date = new Date(System.currentTimeMillis())) = {

      val func = (tmp:java.util.Date)=> {
        val c1 = Calendar.getInstance
        val c2 = Calendar.getInstance
        c2.setTimeInMillis(d.getTime)
        c1.setTime(tmp)
        (c2.get(Calendar.YEAR) - c1.get(Calendar.YEAR) ) * 12 + c2.get(Calendar.MONTH) - c1.get(Calendar.MONTH)
      }
      func(toDate)
    }

    /**
      * 直到指定月份，返回从开始月份到指定月份指定的月份组合，不包含指定月份
      * @param d
      * @return
      */
    def upTo(d: Date = new Date(System.currentTimeMillis())):Seq[Int] = {
      val months = calculate(d)
      val c1 = Calendar.getInstance
      c1.setTime(toDate)
      c1.add(Calendar.MONTH, -1)
      for(i <-0 until months) yield {
          c1.add(Calendar.MONTH, 1)
          sdf6.format(c1.getTime).toInt
      }
    }

    /**
      * 直到指定月份，返回从开始月份到指定月份指定的月份组合，不包含指定月份
      * @param d
      * @return
      */
    def upTo(d: Int):Seq[Int] = {
      val months = calculate(new Date(d.toDate.getTime))
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
    println(i.upTo(201805))
    println(i.upTo())
    println(i.calculate())
  }
}