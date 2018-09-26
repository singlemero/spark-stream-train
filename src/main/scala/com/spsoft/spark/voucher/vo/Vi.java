package com.spsoft.spark.voucher.vo;

import java.math.BigDecimal;
import java.util.Date;

/**
 * voucherItemsId: String,
 *                                voucherId: String,
 *                                companyId: Long,//L
 *                                voucherTime: Date,
 *                                voucherAbstract: String,
 *                                subjectId: Long,
 *                                subjectCode: String,
 *                                subjectFullName: String,
 *                                currencyCode: String,
 *                                exchangeRate: BigDecimal,
 *                                originalAmount: BigDecimal,
 *                                subjectAmount: BigDecimal,
 *                                 voucherDirection: Long,//L
 *                                lendingDirection: Long,//L
 *                                saleUnitName: String,
 *                                qty: Double,
 *                                notaxActPrice: Double,
 *                                subjectSort: String
 */
public class Vi {

    private String voucherItemId;

    private String voucherId;

    private Integer companyId;

    private Date voucherTime;

    private String voucherAbstract;

    private Long subjectId;

    private String subjectCode;

    private String subjectFullName;

    private String currencyCode;

    private BigDecimal exchangeRate;

    private BigDecimal originalAmount;

    private BigDecimal subjectAmount;

    private Integer voucherDirection;

    private Integer lendingDirection;

    private String saleUnitName;

    private Double qty;

    private Double notaxActPrice;

    private String subjectSort;

    public String getVoucherItemId() {
        return voucherItemId;
    }

    public void setVoucherItemId(String voucherItemId) {
        this.voucherItemId = voucherItemId;
    }

    public String getVoucherId() {
        return voucherId;
    }

    public void setVoucherId(String voucherId) {
        this.voucherId = voucherId;
    }

    public Integer getCompanyId() {
        return companyId;
    }

    public void setCompanyId(Integer companyId) {
        this.companyId = companyId;
    }

    public Date getVoucherTime() {
        return voucherTime;
    }

    public void setVoucherTime(Date voucherTime) {
        this.voucherTime = voucherTime;
    }

    public String getVoucherAbstract() {
        return voucherAbstract;
    }

    public void setVoucherAbstract(String voucherAbstract) {
        this.voucherAbstract = voucherAbstract;
    }

    public Long getSubjectId() {
        return subjectId;
    }

    public void setSubjectId(Long subjectId) {
        this.subjectId = subjectId;
    }

    public String getSubjectCode() {
        return subjectCode;
    }

    public void setSubjectCode(String subjectCode) {
        this.subjectCode = subjectCode;
    }

    public String getSubjectFullName() {
        return subjectFullName;
    }

    public void setSubjectFullName(String subjectFullName) {
        this.subjectFullName = subjectFullName;
    }

    public String getCurrencyCode() {
        return currencyCode;
    }

    public void setCurrencyCode(String currencyCode) {
        this.currencyCode = currencyCode;
    }

    public BigDecimal getExchangeRate() {
        return exchangeRate;
    }

    public void setExchangeRate(BigDecimal exchangeRate) {
        this.exchangeRate = exchangeRate;
    }

    public BigDecimal getOriginalAmount() {
        return originalAmount;
    }

    public void setOriginalAmount(BigDecimal originalAmount) {
        this.originalAmount = originalAmount;
    }

    public BigDecimal getSubjectAmount() {
        return subjectAmount;
    }

    public void setSubjectAmount(BigDecimal subjectAmount) {
        this.subjectAmount = subjectAmount;
    }

    public Integer getVoucherDirection() {
        return voucherDirection;
    }

    public void setVoucherDirection(Integer voucherDirection) {
        this.voucherDirection = voucherDirection;
    }

    public Integer getLendingDirection() {
        return lendingDirection;
    }

    public void setLendingDirection(Integer lendingDirection) {
        this.lendingDirection = lendingDirection;
    }

    public String getSaleUnitName() {
        return saleUnitName;
    }

    public void setSaleUnitName(String saleUnitName) {
        this.saleUnitName = saleUnitName;
    }

    public Double getQty() {
        return qty;
    }

    public void setQty(Double qty) {
        this.qty = qty;
    }

    public Double getNotaxActPrice() {
        return notaxActPrice;
    }

    public void setNotaxActPrice(Double notaxActPrice) {
        this.notaxActPrice = notaxActPrice;
    }

    public String getSubjectSort() {
        return subjectSort;
    }

    public void setSubjectSort(String subjectSort) {
        this.subjectSort = subjectSort;
    }
}
