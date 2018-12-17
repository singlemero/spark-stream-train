package exercise.json

import java.io.StringWriter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.gson.Gson
import com.spsoft.spark.voucher.VoucherB
import com.spsoft.spark.voucher.serializer.{DateToLongSerializer, StringToIntSerializer}
import com.spsoft.spark.voucher.vo.{Voucher, VoucherItems}
import exercise.sql.ZZ
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read => sread, write => swrite}

/**
  * json 对象转换
  * jackson和json4s
  */
object JsonExample {

  def testJackson = {
    //object to string
    println("****************** testJackson *******************")
    val origin = ZZ(BigInt(3), BigDecimal(33),"adfadsf")

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val out = new StringWriter
    mapper.writeValue(out, origin)
    val jsonStr = out.toString()
    println(jsonStr)

    //string to object classOf[List[VoucherItems]] for array
    val target = mapper.readValue(jsonStr, classOf[ZZ])
    println(target)
  }

  def testJackson1 = {
    //object to string
    println("****************** testJackson *******************")
    val origin = ZZ(BigInt(3), BigDecimal(33),"adfadsf")

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val out = new StringWriter
    mapper.writeValue(out, origin)
    val jsonStr = out.toString()
    println(jsonStr)
    val str = """{"accountPeriod":"201809","companyId":3217,"items":[{"companyId":3217,"currencyCode":"RMB","exchangeRate":1.000000,"lendingDirection":2,"memoItemIds":[],"notaxActPrice":0E-8,"originalAmount":-333.00,"qty":0E-8,"saleunitName":"","subjectAmount":-333.00,"subjectCode":"60010001","subjectFullName":"主营业务收入-外（免）收入","subjectId":1219295214389987921,"subjectSort":10,"voucherAbstract":"销售收入","voucherDirection":1,"voucherId":1880455538631805824,"voucherItemsId":1880455538631805827,"voucherTime":1538236800000},{"companyId":3217,"currencyCode":"RMB","exchangeRate":1.000000,"lendingDirection":1,"memoItemIds":[],"notaxActPrice":0E-8,"originalAmount":-333.00,"qty":0E-8,"saleunitName":"","subjectAmount":-333.00,"subjectCode":"1531","subjectFullName":"长期应收款","subjectId":1219295214356433490,"subjectSort":11,"voucherAbstract":"支付工资","voucherDirection":2,"voucherId":1880455538631805824,"voucherItemsId":1880455538631805828,"voucherTime":1538236800000}],"voucherId":1880455538631805824}"""
    //string to object classOf[List[VoucherItems]] for array
    import com.fasterxml.jackson.databind.DeserializationFeature
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val target = mapper.readValue(str, classOf[Voucher])
    println(target)
  }

  def testGson = {
    println("****************** testGson *******************")
    val origin = ZZ(BigInt(3), BigDecimal(33),"adfadsf")
    val gson = new Gson()
    println(gson.toJson(origin))
  }

  def testJson4s = {
    println("****************** testJson4s *******************")
    implicit val formats = DefaultFormats + new DateToLongSerializer()
    val str = """[{"voucherItemsId":3591201801310018001,"voucherId":3591201801310018,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;耀华机械公司1.15","subjectId":1532444380036361235,"subjectCode":"60010104","subjectFullName":"主营业务收入-方管方管60—100","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":47521.79,"subjectAmount":47521.79,"voucherDirection":2,"lendingDirection":1,"saleUnitName":"吨","qty":12.93,"notaxActPrice":3675.3125,"subjectSort":7},{"voucherItemsId":3591201801310018002,"voucherId":3591201801310018,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;耀华机械公司1.15","subjectId":1532444380036361241,"subjectCode":"60010105","subjectFullName":"主营业务收入-方管方管101—200","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":45557.47,"subjectAmount":45557.47,"voucherDirection":2,"lendingDirection":1,"saleUnitName":"吨","qty":12.393,"notaxActPrice":3676.0647,"subjectSort":8},{"voucherItemsId":3591201801310018003,"voucherId":3591201801310018,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;耀华机械公司1.15","subjectId":1532444380036361235,"subjectCode":"60010104","subjectFullName":"主营业务收入-方管方管60—100","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":1631.54,"subjectAmount":1631.54,"voucherDirection":2,"lendingDirection":1,"saleUnitName":"吨","qty":0.443,"notaxActPrice":3682.9345,"subjectSort":9},{"voucherItemsId":3591201801310018004,"voucherId":3591201801310018,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;耀华机械公司1.15","subjectId":1532444380036361229,"subjectCode":"60010103","subjectFullName":"主营业务收入-方管方管10—50","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":24605.56,"subjectAmount":24605.56,"voucherDirection":2,"lendingDirection":1,"saleUnitName":"吨","qty":6.695,"notaxActPrice":3675.2143,"subjectSort":10},{"voucherItemsId":3591201801310018005,"voucherId":3591201801310018,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;耀华机械公司1.15","subjectId":1532444380036361241,"subjectCode":"60010105","subjectFullName":"主营业务收入-方管方管101—200","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":69113.67,"subjectAmount":69113.67,"voucherDirection":2,"lendingDirection":1,"saleUnitName":"吨","qty":18.803,"notaxActPrice":3675.6725,"subjectSort":11},{"voucherItemsId":3591201801310018006,"voucherId":3591201801310018,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;耀华机械公司1.15","subjectId":1532444380036361235,"subjectCode":"60010104","subjectFullName":"主营业务收入-方管方管60—100","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":30982.91,"subjectAmount":30982.91,"voucherDirection":2,"lendingDirection":1,"saleUnitName":"吨","qty":8.429,"notaxActPrice":3675.7516,"subjectSort":12},{"voucherItemsId":3591201801310018007,"voucherId":3591201801310018,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;耀华机械公司1.15","subjectId":1532444380036361229,"subjectCode":"60010103","subjectFullName":"主营业务收入-方管方管10—50","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":30897.44,"subjectAmount":30897.44,"voucherDirection":2,"lendingDirection":1,"saleUnitName":"吨","qty":8.406,"notaxActPrice":3675.6412,"subjectSort":13},{"voucherItemsId":3591201801310018008,"voucherId":3591201801310018,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;耀华机械公司1.15","subjectId":1532444380036361241,"subjectCode":"60010105","subjectFullName":"主营业务收入-方管方管101—200","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":31183.76,"subjectAmount":31183.76,"voucherDirection":2,"lendingDirection":1,"saleUnitName":"吨","qty":8.484,"notaxActPrice":3675.5964,"subjectSort":14},{"voucherItemsId":3591201801310018009,"voucherId":3591201801310018,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;耀华机械公司1.15","subjectId":1532444380036361235,"subjectCode":"60010104","subjectFullName":"主营业务收入-方管方管60—100","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":25921.37,"subjectAmount":25921.37,"voucherDirection":2,"lendingDirection":1,"saleUnitName":"吨","qty":7.053,"notaxActPrice":3675.2261,"subjectSort":15},{"voucherItemsId":3591201801310018010,"voucherId":3591201801310018,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;耀华机械公司1.15","subjectId":1532444380036361235,"subjectCode":"60010104","subjectFullName":"主营业务收入-方管方管60—100","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":29629.14,"subjectAmount":29629.14,"voucherDirection":2,"lendingDirection":1,"saleUnitName":"吨","qty":8.061,"notaxActPrice":3675.6159,"subjectSort":16},{"voucherItemsId":3591201801310018011,"voucherId":3591201801310018,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;耀华机械公司1.15","subjectId":1532431797761895542,"subjectCode":"222100010002","subjectFullName":"应交税费-应交增值税-销项税额","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":73475.13,"subjectAmount":73475.13,"voucherDirection":2,"lendingDirection":1,"saleUnitName":null,"qty":0.0,"notaxActPrice":0.0,"subjectSort":17},{"voucherItemsId":3591201801310018012,"voucherId":3591201801310018,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;耀华机械公司1.16","subjectId":1532439444682730496,"subjectCode":"10020101","subjectFullName":"银行存款-农行_490100000000","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":325546.29,"subjectAmount":325546.29,"voucherDirection":1,"lendingDirection":1,"saleUnitName":null,"qty":0.0,"notaxActPrice":0.0,"subjectSort":1},{"voucherItemsId":3591201801310018013,"voucherId":3591201801310018,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;耀华机械公司1.16","subjectId":1532439444682730496,"subjectCode":"10020101","subjectFullName":"银行存款-农行_490100000000","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":132339.25,"subjectAmount":132339.25,"voucherDirection":1,"lendingDirection":1,"saleUnitName":null,"qty":0.0,"notaxActPrice":0.0,"subjectSort":2},{"voucherItemsId":3591201801310018014,"voucherId":3591201801310018,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;耀华机械公司1.16","subjectId":1532439481592605696,"subjectCode":"11220357","subjectFullName":"应收账款-四会市耀华精密机械有限公司","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":47796.2,"subjectAmount":47796.2,"voucherDirection":1,"lendingDirection":1,"saleUnitName":null,"qty":0.0,"notaxActPrice":0.0,"subjectSort":3},{"voucherItemsId":3591201801310018015,"voucherId":3591201801310018,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;耀华机械公司1.15","subjectId":1532444380036361235,"subjectCode":"60010104","subjectFullName":"主营业务收入-方管方管60—100","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":64614.1,"subjectAmount":64614.1,"voucherDirection":2,"lendingDirection":1,"saleUnitName":"吨","qty":17.581,"notaxActPrice":3675.2233,"subjectSort":4},{"voucherItemsId":3591201801310018016,"voucherId":3591201801310018,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;耀华机械公司1.15","subjectId":1532444380036361241,"subjectCode":"60010105","subjectFullName":"主营业务收入-方管方管101—200","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":26938.46,"subjectAmount":26938.46,"voucherDirection":2,"lendingDirection":1,"saleUnitName":"吨","qty":7.329,"notaxActPrice":3675.5983,"subjectSort":5},{"voucherItemsId":3591201801310018017,"voucherId":3591201801310018,"companyId":3591,"voucherTime":1517328000000,"voucherAbstract":"销售;耀华机械公司1.15","subjectId":1532444380036361229,"subjectCode":"60010103","subjectFullName":"主营业务收入-方管方管10—50","currencyCode":"RMB","exchangeRate":1.0,"originalAmount":3609.4,"subjectAmount":3609.4,"voucherDirection":2,"lendingDirection":1,"saleUnitName":"吨","qty":1.0,"notaxActPrice":3609.4,"subjectSort":6}]"""
    val target = sread[List[VoucherItems]](str)
    println(target)
  }


  def testJson4s1 = {
    println("****************** testJson4s *******************")
    import com.fasterxml.jackson.databind.SerializationFeature
    //import org.json4s.jackson.JsonMethods._
    import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
    org.json4s.jackson.JsonMethods.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    implicit val formats = DefaultFormats + new DateToLongSerializer() + new StringToIntSerializer()
    val str = """{"accountPeriod":"201809","companyId":3217,"items":[{"companyId":3217,"currencyCode":"RMB","exchangeRate":1.000000,"lendingDirection":2,"memoItemIds":[],"notaxActPrice":0E-8,"originalAmount":-333.00,"qty":0E-8,"saleunitName":"","subjectAmount":-333.00,"subjectCode":"60010001","subjectFullName":"主营业务收入-外（免）收入","subjectId":1219295214389987921,"subjectSort":10,"voucherAbstract":"销售收入","voucherDirection":1,"voucherId":1880455538631805824,"voucherItemsId":1880455538631805827,"voucherTime":1538236800000},{"companyId":3217,"currencyCode":"RMB","exchangeRate":1.000000,"lendingDirection":1,"memoItemIds":[],"notaxActPrice":0E-8,"originalAmount":-333.00,"qty":0E-8,"saleunitName":"","subjectAmount":-333.00,"subjectCode":"1531","subjectFullName":"长期应收款","subjectId":1219295214356433490,"subjectSort":11,"voucherAbstract":"支付工资","voucherDirection":2,"voucherId":1880455538631805824,"voucherItemsId":1880455538631805828,"voucherTime":1538236800000}],"voucherId":1880455538631805824}"""
    val target = sread[Voucher](str)
    //val target = str.extract[Voucher]
    println(target)
  }

  def main(args: Array[String]): Unit = {
    //testGson
    testJackson1
    //testJson4s1
  }
}
