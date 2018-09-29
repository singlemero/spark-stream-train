package com.spsoft.spark.sql

import org.apache.commons.lang3.StringUtils

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * 多行数据转列
  */
class ColumnToRowFunc extends UserDefinedAggregateFunction{

  private val quote = ","

  /**
    * 定义输入类型
    * @return
    */
  override def inputSchema: StructType = StructType(StructField("inputColumn", StringType) :: Nil)

  /**
    * 定义中间类型，可以多个
    * @return
    */
  override def bufferSchema: StructType =  StructType(StructField("sum", StringType)  :: Nil)

  /**
    * 定义结果类型
    * @return
    */
  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  /**
    * 设置初始值
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //buffer(0) = StringUtils.EMPTY
  }

  /**
    * 定义更新方法
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = if(buffer.isNullAt(0))  input.getString(0) else buffer.getString(0) + quote + input.getString(0)
    }
  }

  /**
    * 定义最终合并
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = if(buffer1.isNullAt(0)) {
      if(buffer2.isNullAt(0)) StringUtils.EMPTY else buffer2.getString(0)
    } else buffer1.getString(0) + quote + buffer2.getString(0)
  }

  /**
    * 定义最终的取值
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = buffer.getString(0)
}
