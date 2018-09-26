package com.spsoft.spark.voucher.vo

import java.sql.{Date, Timestamp}


/**
  *
  * @param voucherItemsId 凭证细单ID
  * @param voucherId 凭证主单ID
  * @param companyId 公司ID
  * @param voucherTime 凭证日期（分区键）
  * @param voucherAbstract 凭证摘要
  * @param subjectId 科目ID
  * @param subjectCode 科目代码
  * @param subjectFullName 科目全称
  * @param currencyCode 币别编码
  * @param exchangeRate 汇率
  * @param originalAmount 原币金额
  * @param subjectAmount 发生金额
  * @param voucherDirection 凭证借贷方向（1：借、2：贷）
  * @param lendingDirection 科目表的借贷方向（1：借、2：贷）
  * @param saleUnitName 单位名称
  * @param qty 数量
  * @param notaxActPrice 不含税终计单价
  * @param subjectSort 排列序号
  */
case class VoucherItems(voucherItemsId: BigInt,
                               voucherId: BigInt,
                               companyId: Int,//L
                               voucherTime: Date,
                               voucherAbstract: String,
                               subjectId: BigInt,
                               subjectCode: String,
                               subjectFullName: String,
                               currencyCode: String,
                               exchangeRate: BigDecimal,
                               originalAmount: BigDecimal,
                               subjectAmount: BigDecimal,
                                voucherDirection: Int,//L
                               lendingDirection: Int,//L
                               saleUnitName: String,
                               qty: BigDecimal,
                               notaxActPrice: BigDecimal,
                               subjectSort: Int)


/**
  * `SUBJECT_ID` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '科目ID',
  * `SUBJECT_CODE` varchar(20) NOT NULL COMMENT '科目编码',
  * `SUBJECT_NAME` varchar(50) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '科目名称',
  * `SUBJECT_FULL_NAME` varchar(160) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '科目全称（科目递归路径）',
  * `SUBJECT_CATEGORY` tinyint(4) NOT NULL COMMENT '科目分类（1：资产、2：负债、3：权益、4：损益、5：成本）',
  * `LENDING_DIRECTION` tinyint(4) NOT NULL COMMENT '余额方向（1：借、2：贷）',
  * `INITIAL_AMOUNT` decimal(10,2) DEFAULT NULL COMMENT '初始余额',
  * `SUBJECT_PARENT_CODE` varchar(20) NOT NULL COMMENT '上级科目',
  * `SUBJECT_NATURE` tinyint(4) DEFAULT NULL COMMENT '科目性质(0: 纳税人-共有；1：增值税--一般纳税人-特有；2: 增值税--小规模纳税人-特有)',
  * `ENABLED_MODIFY` tinyint(4) NOT NULL COMMENT '可否修改（1：是、0：否）',
  * `ALLOW_LEVEL` tinyint(4) DEFAULT NULL COMMENT '科目允许层级',
  * `CREATE_USERID` int(11) NOT NULL COMMENT '创建人员',
  * `CREATE_TIME` datetime NOT NULL COMMENT '创建时间',
  * `MODIFY_USERID` int(11) DEFAULT NULL COMMENT '修改人员',
  * `MODIFY_TIME` datetime DEFAULT NULL COMMENT '修改时间',
  * accountPeriod 会计属期，额外添加
  */
case class SubjectInfo(subjectId: String, subjectCode: String, subjectName: String, subjectFullName: String, subjectCategory: Int, lendingDirection: Int,
                       initialAmount: Double, subjectParentCode: String, subjectNature: Int, enabledModify: Int,
                       createUserId: Long, createTime: Date, modifyUserId: Long, modifyTime: Date, accountPeriod: Int)


case class SubjectBalancePrimary(companyId: Int, subjectCode: String,  accountPeriod: Int)

/**
  * 仅用于插入
  * @param companyId
  * @param subjectId
  * @param subjectCode
  * @param subjectName
  * @param subjectFullName
  * @param subjectCategory
  * @param lendingDirection
  * @param initialAmount
  * @param initialQty
  * @param subjectParentCode
  * @param accountPeriod
  * @param accountPeriodEnd
  */
case class SubjectInfoBrief(companyId: Int,
                           subjectId: String,
                           subjectCode: String,
                           subjectName: String,
                           subjectFullName: String,
                           subjectCategory: Int,
                           lendingDirection: Int,
                           initialAmount: BigDecimal,
                           initialQty: BigDecimal,
                           subjectParentCode: String,
                           accountPeriod: Int,
                           accountPeriodEnd: Int)

/**
  * 凭证主单
  * @param id
  * @param items
  */
case class Voucher(id: BigInt,companyId: Int, accountPeriod: Int, codes: String, items: List[VoucherItems])


/**
  * |  `sequenceId` bigint(20) not null comment '序号',
  * |  `companyId` int(11) not null comment '公司id',
  * |  `accountPeriod` int(11) not null comment '会计属期（格式：yyyymm）',
  * |  `subjectId` bigint(20) not null comment '科目id',
  * |  `subjectCode` varchar(12) not null comment '科目编码',
  * |  `subjectName` varchar(50) not null comment '科目名称',
  * |  `subjectFullName` varchar(160) not null comment '科目全称（科目递归路径）',
  * |  `subjectCategory` tinyint(4) not null comment '科目分类（1：资产、2：负债、3：权益、4：损益）',
  * |  `lendingDirection` tinyint(4) default null comment '科目表的借贷方向（1：借、2：贷）',
  * |  `initialQty` decimal(20,8) default null comment '期初数量',
  * |  `initialDebitAmount` decimal(16,2) default null comment '期初借方余额',
  * |  `initialCreditAmount` decimal(16,2) default null comment '期初贷方余额',
  * |  `currentDebitAmount` decimal(16,2) default null comment '本期借方金额',
  * |  `currentDebitQty` decimal(20,8) default null comment '本期借方数量',
  * |  `currentCreditAmount` decimal(16,2) default null comment '本期贷方金额',
  * |  `currentCreditQty` decimal(20,8) default null comment '本期贷方数量',
  * |  `currentDebitNocarryAmount` decimal(16,2) default null comment '本期借方金额(不包括结转)',
  * |  `currentCreditNocarryAmount` decimal(16,2) default null comment '本期贷方金额(不包括结转)',
  * |  `endingDebitAmount` decimal(16,2) default null comment '期末借方余额',
  * |  `endingCreditAmount` decimal(16,2) default null comment '期末贷方余额',
  * |  `endingQty` decimal(20,8) default null comment '期末数量',
  * |  `yearDebitAmount` decimal(16,2) default null comment '本年累计借方金额',
  * |  `yearCreditAmount` decimal(16,2) default null comment '本年累计贷方金额',
  * |  `yearCreditQty` decimal(20,8) default null comment '本年累计贷方数量',
  * |  `yearDebitQty` decimal(20,8) default null comment '本年累计借方数量',
  * |  `yearDebitNocarryAmount` decimal(16,2) default null comment '本年累计借方金额(不包括结转)',
  * |  `yearCreditNocarryAmount` decimal(16,2) default null comment '本年累计贷方金额(不包括结转)',
  * |  `subjectParentCode` varchar(20) not null comment '上级科目',
  * |  `createTime` datetime not null comment '创建时间',
  */
case class SubjectBalance(sequenceId: BigInt, companyId: Int,
                          accountPeriod: Int, subjectId: String,
                          subjectCode: String, subjectName: String,
                          subjectFullName: String,
                          subjectCategory: Int,
                          lendingDirection: Int,
                          initialQty: BigDecimal,
                          initialDebitAmount: BigDecimal,
                          initialCreditAmount: BigDecimal,
                          currentDebitAmount: BigDecimal,
                          currentDebitQty: BigDecimal,
                          currentCreditAmount: BigDecimal,
                          currentCreditQty: BigDecimal,
                          currentDebitNocarryAmount: BigDecimal,
                          currentCreditNocarryAmount: BigDecimal,
                          endingDebitAmount: BigDecimal,
                          endingCreditAmount: BigDecimal,
                          endingQty: BigDecimal,
                          yearDebitAmount: BigDecimal,
                          yearDebitQty: BigDecimal,
                          yearCreditAmount: BigDecimal,
                          yearCreditQty: BigDecimal,
                          yearDebitNocarryAmount: BigDecimal,
                          yearCreditNocarryAmount: BigDecimal,
                          subjectParentCode: String,
                          createTime: Timestamp
                         )

/**
  * 此对象由 Voucher 生成
  * @param companyId
  * @param accountPeriod
  * @param subjectCode
  * @param currentDebitAmount
  * @param currentDebitQty
  * @param currentCreditAmount
  * @param currentCreditQty
  * @param currentDebitNocarryAmount
  * @param currentCreditNocarryAmount
  */
case class SubjectBalanceSlim(companyId: Int,
                              accountPeriod: Int,
                              subjectCode: String,
                              currentDebitAmount: BigDecimal, //本期借
                              currentDebitQty: BigDecimal, //本期借数量
                              currentCreditAmount: BigDecimal, //本期贷
                              currentCreditQty: BigDecimal, //本期贷数量
                              currentDebitNocarryAmount: BigDecimal, //本期借（不含结转）
                              currentCreditNocarryAmount: BigDecimal //本期贷（不含结转）
                             )



/**
  * 此对象由 SubjectBalanceSlim 生成
  * @param companyId
  * @param accountPeriod
  * @param subjectCode
  * @param initialDebitAmount
  * @param initialCreditAmount
  * @param initialQty
  * @param currentDebitAmount
  * @param currentDebitQty
  * @param currentDebitNocarryAmount
  * @param currentCreditAmount
  * @param currentCreditQty
  * @param currentCreditNocarryAmount
  * @param endingDebitAmount
  * @param endingCreditAmount
  * @param endingQty
  * @param yearDebitAmount
  * @param yearDebitQty
  * @param yearDebitNocarryAmount
  * @param yearCreditAmount
  * @param yearCreditQty
  * @param yearCreditNocarryAmount
  */
case class SubjectBalanceMedium(//3
                                 companyId: Int,
                              accountPeriod: Int,
                              subjectCode: String,
//2 期初
                              initialDebitAmount: BigDecimal, //期初借  来源：上月份本期借+
                              initialCreditAmount: BigDecimal,//期初贷       上月份本期贷+
                              initialQty: BigDecimal, //期初数量             上月份期末数量
//6 本期
                              currentDebitAmount: BigDecimal, //本期借
                              currentDebitQty: BigDecimal, //本期借数量
                              currentDebitNocarryAmount: BigDecimal, //本期借（不含结转）

                              currentCreditAmount: BigDecimal, //本期贷
                              currentCreditQty: BigDecimal, //本期贷数量
                              currentCreditNocarryAmount: BigDecimal, //本期贷（不含结转）
//3 期末
                              endingDebitAmount: BigDecimal, //期末借        上月份本期借+
                              endingCreditAmount: BigDecimal,//期末贷        上月份本期贷+
                              endingQty: BigDecimal,//期末数量                上月份本期发生数量+
   //6 本年
                              yearDebitAmount: BigDecimal,//本年累计借        上月份本期借+
                              yearDebitQty: BigDecimal,//本年累计借数量        上月份本期借数量+
                              yearDebitNocarryAmount: BigDecimal,//本年累计借方金额(不包括结转)  上月份本期借（不含结转）+

                              yearCreditAmount: BigDecimal,//本年累计贷       上月份本期贷+
                              yearCreditQty: BigDecimal,//本年累计贷数量       上月份本期贷数量+
                              yearCreditNocarryAmount: BigDecimal//本年累计贷（不包括结转）      上月份本期贷（不含结转 +
                             )

