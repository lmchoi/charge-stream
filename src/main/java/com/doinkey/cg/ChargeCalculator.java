package com.doinkey.cg;

public class ChargeCalculator {
    public Charge calculate(Transaction t) {
        return new Charge(t.getTxnId());
    }
}
