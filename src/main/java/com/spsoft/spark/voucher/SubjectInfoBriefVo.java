package com.spsoft.spark.voucher;


import java.math.BigDecimal;

public class SubjectInfoBriefVo {

    private Integer companyId;

    private String subjectId;

    private String subjectCode;

    private String subjectName;

    private String subjectFullName;

    private Integer subjectCategory;

    private Integer lendingDirection;

    private BigDecimal initialAmount;

    private BigDecimal initialQty;

    private String subjectParentCode;

    private Integer accountPeriod;

    private Integer accountPeriodEnd;

    public Integer getCompanyId() {
        return companyId;
    }

    public void setCompanyId(Integer companyId) {
        this.companyId = companyId;
    }

    public String getSubjectId() {
        return subjectId;
    }

    public void setSubjectId(String subjectId) {
        this.subjectId = subjectId;
    }

    public String getSubjectCode() {
        return subjectCode;
    }

    public void setSubjectCode(String subjectCode) {
        this.subjectCode = subjectCode;
    }

    public String getSubjectName() {
        return subjectName;
    }

    public void setSubjectName(String subjectName) {
        this.subjectName = subjectName;
    }

    public String getSubjectFullName() {
        return subjectFullName;
    }

    public void setSubjectFullName(String subjectFullName) {
        this.subjectFullName = subjectFullName;
    }

    public Integer getSubjectCategory() {
        return subjectCategory;
    }

    public void setSubjectCategory(Integer subjectCategory) {
        this.subjectCategory = subjectCategory;
    }

    public Integer getLendingDirection() {
        return lendingDirection;
    }

    public void setLendingDirection(Integer lendingDirection) {
        this.lendingDirection = lendingDirection;
    }

    public BigDecimal getInitialAmount() {
        return initialAmount;
    }

    public void setInitialAmount(BigDecimal initialAmount) {
        this.initialAmount = initialAmount;
    }

    public BigDecimal getInitialQty() {
        return initialQty;
    }

    public void setInitialQty(BigDecimal initialQty) {
        this.initialQty = initialQty;
    }

    public String getSubjectParentCode() {
        return subjectParentCode;
    }

    public void setSubjectParentCode(String subjectParentCode) {
        this.subjectParentCode = subjectParentCode;
    }

    public Integer getAccountPeriod() {
        return accountPeriod;
    }

    public void setAccountPeriod(Integer accountPeriod) {
        this.accountPeriod = accountPeriod;
    }

    public Integer getAccountPeriodEnd() {
        return accountPeriodEnd;
    }

    public void setAccountPeriodEnd(Integer accountPeriodEnd) {
        this.accountPeriodEnd = accountPeriodEnd;
    }
}
