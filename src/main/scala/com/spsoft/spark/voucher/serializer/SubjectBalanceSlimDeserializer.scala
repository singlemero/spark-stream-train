package com.spsoft.spark.voucher.serializer

import java.io.UnsupportedEncodingException
import java.util

import com.spsoft.spark.voucher.vo.SubjectBalanceSlim
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

/**
  * 科目余额表反序列化
  */
class SubjectBalanceSlimDeserializer extends Deserializer[SubjectBalanceSlim]{

  private var encoding = "UTF8"
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    val propertyName = if (isKey) "key.deserializer.encoding"
    else "value.deserializer.encoding"
    var encodingValue = configs.get(propertyName)
    if (encodingValue == null) encodingValue = configs.get("deserializer.encoding")
    if (encodingValue != null && encodingValue.isInstanceOf[String]) encoding = encodingValue.asInstanceOf[String]
  }

  override def deserialize(topic: String, data: Array[Byte]): SubjectBalanceSlim = {
    implicit val formats = DefaultFormats + new DateToLongSerializer
    try
        if (data == null) return null
        else return read[SubjectBalanceSlim](new String(data, encoding))
    catch {
      case e: UnsupportedEncodingException =>
        throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding " + encoding)
    }
  }

  override def close(): Unit = {}
}
