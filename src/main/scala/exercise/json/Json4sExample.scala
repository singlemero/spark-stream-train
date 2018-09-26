package exercise.json

import java.sql.Date
import java.util.StringJoiner

import com.spsoft.spark.voucher.vo.{DateToLongSerializer, Voucher}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read, write}

object Json4sExample {

  /**
  class DateSerializer extends CustomSerializer[Date](format => (
    {
      case JLong(x) => new Date(x.longValue())
      //case JsNumber(x) => new Date(x.longValue())
    },
    {
      case x: Date => JLong(x.getTime)
    }
  ))
    */

  case class A(id:BigDecimal, birthday: Date)

  def main(args: Array[String]): Unit = {

    implicit val formats = DefaultFormats + new DateToLongSerializer

    println(new Date(System.currentTimeMillis()))

    val person = new A(BigDecimal(88), new Date(System.currentTimeMillis()))

    val d = new Date(System.currentTimeMillis())
    printf(write(person))

    val str = """{"id":3591201801310003,"items":[{"voucherItemsId":3591201801310003001,"voucherId":3591201801310003,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;容桂海尾商业贸易部","subjectId":1532431797761895424,"subjectCode":"1001","subjectFullName":"库存现金","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":4422.0,"subjectAmount":4422.0,"voucherDirection":1,"lendingDirection":1,"saleUnitName":null,"qty":0.0,"notaxActPrice":0.0,"subjectSort":1},{"voucherItemsId":3591201801310003002,"voucherId":3591201801310003,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;容桂海尾商业贸易部1.15","subjectId":1532444380036361229,"subjectCode":"60010103","subjectFullName":"主营业务收入-方管方管10—50","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":3779.48,"subjectAmount":3779.48,"voucherDirection":2,"lendingDirection":1,"saleUnitName":"吨","qty":1.381,"notaxActPrice":2736.7705,"subjectSort":2},{"voucherItemsId":3591201801310003003,"voucherId":3591201801310003,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;容桂海尾商业贸易部1.15","subjectId":1532431797761895542,"subjectCode":"222100010002","subjectFullName":"应交税费-应交增值税-销项税额","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":642.52,"subjectAmount":642.52,"voucherDirection":2,"lendingDirection":1,"saleUnitName":null,"qty":0.0,"notaxActPrice":0.0,"subjectSort":3}]}"""

    val voucher = read[Voucher](str)

    //val stringBuilder ts = new StringBuilder
    val subjectCode = voucher.items.map(i=> {
      val sb = new StringJoiner(",")
      for(a <- 1 to i.subjectCode.length if a % 4 == 0){
        sb.add(i.subjectCode.take(a))
      }
      sb.toString
    }).reduce[String]((a,b)=> String.join(",", a, b))

    printf(subjectCode)
    //printf("")
    val test = "222100010002"
    val sb = new StringJoiner(",")
    for(a <- 1 to test.length if a % 4 == 0){
      println(a)
      println(test.take(a))
      //sb.add(test.take(a))
      sb.add(test.take(a))
    }
    //val sj = new StringJoiner(",")

    println(sb.toString)
    //printf(ss)
    //voucher.items.map(i=> i.subjectCode).foreach(printf(_))
  }
}
