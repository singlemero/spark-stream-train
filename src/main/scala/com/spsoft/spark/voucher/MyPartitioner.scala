package com.spsoft.spark.voucher

import com.spsoft.spark.voucher.vo.SubjectBalanceSlim
import org.apache.spark.Partitioner

class MyPartitioner (val num: Int) extends Partitioner{

  private var count:Int = 0;

  override def numPartitions: Int = num
  /**
    * `SEQUENCE_ID` bigint(20) NOT NULL COMMENT '序号',
    * `COMPANY_ID` int(11) NOT NULL COMMENT '公司ID',
    * `ACCOUNT_PERIOD` int(11) NOT NULL COMMENT '会计属期（格式：YYYYMM）',
    * `SUBJECT_ID` bigint(20) NOT NULL COMMENT '科目ID',
    * `SUBJECT_CODE` varchar(12) NOT NULL COMMENT '科目编码',
    * `SUBJECT_NAME` varchar(50) NOT NULL COMMENT '科目名称',
    * `SUBJECT_FULL_NAME` varchar(160) NOT NULL COMMENT '科目全称（科目递归路径）',
    * `SUBJECT_CATEGORY` tinyint(4) NOT NULL COMMENT '科目分类（1：资产、2：负债、3：权益、4：损益）',
    * `LENDING_DIRECTION` tinyint(4) DEFAULT NULL COMMENT '科目表的借贷方向（1：借、2：贷）',
    * `INITIAL_QTY` decimal(20,8) DEFAULT NULL COMMENT '期初数量',
    * `INITIAL_DEBIT_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '期初借方余额',
    * `INITIAL_CREDIT_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '期初贷方余额',
    * `CURRENT_DEBIT_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '本期借方金额',
    * `CURRENT_DEBIT_QTY` decimal(20,8) DEFAULT NULL COMMENT '本期借方数量',
    * `CURRENT_CREDIT_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '本期贷方金额',
    * `CURRENT_CREDIT_QTY` decimal(20,8) DEFAULT NULL COMMENT '本期贷方数量',
    * `CURRENT_DEBIT_NOCARRY_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '本期借方金额(不包括结转)',
    * `CURRENT_CREDIT_NOCARRY_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '本期贷方金额(不包括结转)',
    * `ENDING_DEBIT_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '期末借方余额',
    * `ENDING_CREDIT_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '期末贷方余额',
    * `ENDING_QTY` decimal(20,8) DEFAULT NULL COMMENT '期末数量',
    * `YEAR_DEBIT_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '本年累计借方金额',
    * `YEAR_CREDIT_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '本年累计贷方金额',
    * `YEAR_CREDIT_QTY` decimal(20,8) DEFAULT NULL COMMENT '本年累计贷方数量',
    * `YEAR_DEBIT_QTY` decimal(20,8) DEFAULT NULL COMMENT '本年累计借方数量',
    * `YEAR_DEBIT_NOCARRY_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '本年累计借方金额(不包括结转)',
    * `YEAR_CREDIT_NOCARRY_AMOUNT` decimal(16,2) DEFAULT NULL COMMENT '本年累计贷方金额(不包括结转)',
    * `SUBJECT_PARENT_CODE` varchar(20) NOT NULL COMMENT '上级科目',
    * `CREATE_TIME` datetime NOT NULL COMMENT '创建时间',
    *
    * @param s
    * @return
    */
  override def getPartition(key: Any): Int = {
    //println(count = count + 1)
    key match {
      case x: SubjectBalanceSlim => {
        val s = s"${x.companyId}${x.accountPeriod}${x.subjectCode}"
        s.hashCode % num
      }
      case x:Long => Math.round(x % num)
      case x: Any => {
        println("any " + x)
        x.hashCode() % num
      }
    }
  }
}
