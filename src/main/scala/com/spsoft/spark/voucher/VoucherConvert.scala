package com.spsoft.spark.voucher

import java.sql.Date

import com.spsoft.spark.voucher.vo.VoucherItems
import org.apache.spark.sql._
object VoucherConvert {


  def main(args: Array[String]): Unit = {
    val current = System.currentTimeMillis()
    val date = new Date(current)
    val l = date.getTime.toString
    printf(s"$l == $current")
    new Date(current)
  }

  implicit class toVoucher(dataFrame: DataFrame){

    def as(cls: Class[VoucherItems]): Dataset[VoucherItems] = {
      implicit val rowEncoder = Encoders.kryo[VoucherItems]
      //dataFrame.map(r => VoucherItems())
      //dataFrame.map((r => VoucherItems(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(12), r(13), r(14), r(15),
      //VoucherItems("","").
      null
    }
  }

}
