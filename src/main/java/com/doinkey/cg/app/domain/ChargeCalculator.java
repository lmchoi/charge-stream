package com.doinkey.cg.app.domain;

import com.doinkey.cg.app.Charge;
import com.doinkey.cg.app.Transaction;

public class ChargeCalculator {
    public Charge calculate(Transaction t) {
        return new Charge(t.getTxnId());
    }
}
