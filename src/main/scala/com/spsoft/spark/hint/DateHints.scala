package com.spsoft.spark.hint

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar

object DateHints {

    implicit class DateMonths(val d : Date){   //隐式类

      private val sdf8 = new SimpleDateFormat("yyyyMMdd")

      private val sdf6 = new SimpleDateFormat("yyyyMM")

      private val sdf4 = new SimpleDateFormat("yyyy")

      def nextMonth(interval: Int = 1):Int = {
        val cal = Calendar.getInstance()
        cal.setTime(d)
        cal.add(Calendar.MONTH, interval)
        sdf6.format(cal.getTime).toInt
      }

      def differ(date: Date): Int = {
        val c1 = Calendar.getInstance
        val c2 = Calendar.getInstance

        c1.setTime(d)
        c2.setTime(date)
        c2.get(Calendar.MONTH) - c1.get(Calendar.MONTH)
      }

      def month():Int = {
        sdf6.format(d).toInt
      }

      def prevMonth(interval:Int = -1):Int = {
        val cal = Calendar.getInstance()
        cal.setTime(d)
        cal.add(Calendar.MONTH, -1)
        sdf6.format(cal.getTime).toInt
      }
    }

  def main(args: Array[String]): Unit = {

    val d = new Date(System.currentTimeMillis())
    println(d.prevMonth())
    val l = System.currentTimeMillis()
    println(l)
    println(new Date(System.currentTimeMillis()).getTime)
  }
}
