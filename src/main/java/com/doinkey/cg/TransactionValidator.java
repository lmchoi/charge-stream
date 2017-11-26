package com.doinkey.cg;

public class TransactionValidator {
    public final static String BAD_KEY_MESSAGE = "That key was BAD";
    private final String BAD_KEY = "bad";

    public boolean isKeyBad(String key) {
        return BAD_KEY.equals(key);
    }
}
