package com.spsoft.spark.voucher;

import com.spsoft.spark.voucher.vo.VoucherItems;

import java.util.List;

public class VoucherB {

    //voucherId: BigInt,companyId: Int, voucherTime: Int, items: List[VoucherItems]
    private long voucherId;

    private long companyId;

    private long voucherTime;

    private List<VoucherItems> items;

    public long getVoucherId() {
        return voucherId;
    }

    public void setVoucherId(long voucherId) {
        this.voucherId = voucherId;
    }

    public long getCompanyId() {
        return companyId;
    }

    public void setCompanyId(long companyId) {
        this.companyId = companyId;
    }

    public long getVoucherTime() {
        return voucherTime;
    }

    public void setVoucherTime(long voucherTime) {
        this.voucherTime = voucherTime;
    }

    public List<VoucherItems> getItems() {
        return items;
    }

    public void setItems(List<VoucherItems> items) {
        this.items = items;
    }
}
