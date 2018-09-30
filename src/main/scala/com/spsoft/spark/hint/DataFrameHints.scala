package com.spsoft.spark.hint

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object DataFrameHints {

  implicit class Improvement(val d : DataFrame) { //隐式类


    private def rename(dataFrame: DataFrame, t: Array[Tuple2[String, String]]): DataFrame = {
      if(t.isEmpty){
        dataFrame
      }else{
        rename(dataFrame.withColumnRenamed(t.head._1, t.head._2), t.tail)
      }
    }


    def convertNames() = {
      val col = d.columns.filter(_.indexOf('_')> -1).map(n => {
          var newName = new mutable.StringBuilder()
          var skip = false
          for (a <- n) {
            if (a == '_') {
              skip = true
            } else {
              newName += (if (skip) a.toUpper else a.toLower)
              skip = false
            }
          }
          (n, newName.toString())

      })
      //col.map(m=>(d, m._1)).re
      rename(d, col)
    }



  }
  def main(args: Array[String]): Unit = {

    val name = "MODIFY_TIME"
    var newName = new mutable.StringBuilder()
    var skip = false
    for (a <- name) {
      if (a == '_') {
        skip = true
      } else {
        newName += (if (skip) a.toUpper else a.toLower)
        skip = false
      }
    }
    println(newName.toString())
  }
}
