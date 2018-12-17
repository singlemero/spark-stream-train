package com.spsoft.spark.voucher.serializer

import org.json4s.{CustomSerializer, JInt, JLong, JString}

class StringToIntSerializer extends CustomSerializer[Int] (format => (
  {
    case JString(x) => x.toInt
    case JInt(x) => x.toInt
  },
  {
    case x: String => JInt(x.toInt)
    case x: Int => JInt(x)
  }
))
