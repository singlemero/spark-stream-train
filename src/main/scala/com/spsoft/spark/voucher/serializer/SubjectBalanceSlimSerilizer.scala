package com.spsoft.spark.voucher.serializer

import java.util

import com.spsoft.spark.voucher.vo.SubjectBalanceSlim
import org.apache.kafka.common.serialization.Serializer
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

/**
  * 序列化，理论上用字符串序列化可满足
  */
class SubjectBalanceSlimSerilizer  extends Serializer[SubjectBalanceSlim]{
  private var encoding = "UTF8"

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    val propertyName = if (isKey) "key.serializer.encoding"
    else "value.serializer.encoding"
    var encodingValue = configs.get(propertyName)
    if (encodingValue == null) encodingValue = configs.get("serializer.encoding")
    if (encodingValue != null && encodingValue.isInstanceOf[String]) encoding = encodingValue.asInstanceOf[String]
  }

  override def serialize(topic: String, data: SubjectBalanceSlim): Array[Byte] = {
    implicit val formats = DefaultFormats + new DateToLongSerializer
    write(data).getBytes(encoding)
  }

  override def close(): Unit = {}
}
