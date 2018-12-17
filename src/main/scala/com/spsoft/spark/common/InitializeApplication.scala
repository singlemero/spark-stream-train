package com.spsoft.spark.common

import java.util.function.BiConsumer

import com.spsoft.spark.utils.PropertiesLoader

object InitializeApplication extends PropertiesLoader{
  override def inputStream: String = ProjectConstants.APPLICATION_PATH

  //val bi:BiConsumer[String, String] = (a:String,b:String)=>{println(s"${a} ${b}")}

  //properties.forEach(bi)

}
