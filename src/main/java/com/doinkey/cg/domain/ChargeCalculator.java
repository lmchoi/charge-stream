package com.doinkey.cg.domain;

import com.doinkey.cg.Charge;
import com.doinkey.cg.Transaction;

public class ChargeCalculator {
    public Charge calculate(Transaction t) {
        return new Charge(t.getTxnId());
    }
}
