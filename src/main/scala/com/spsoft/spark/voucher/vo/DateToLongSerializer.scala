package com.spsoft.spark.voucher.vo

import java.sql.Date

import org.json4s
import org.json4s.{CustomSerializer, Formats, JLong, JInt, JString, MappingException}

class DateToLongSerializer extends CustomSerializer[Date] (format => (
  {
    case JLong(x) => new Date(x.longValue())
    case JInt(x) => new Date(x.toLong)
    case JString(x) => if( x.length > 0) new Date(System.currentTimeMillis()) else new Date(System.currentTimeMillis())
  },
  {
    case x: Date => JLong(x.getTime)
  }
)) //with Serializable