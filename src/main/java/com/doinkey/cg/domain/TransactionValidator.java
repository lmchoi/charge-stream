package com.doinkey.cg.domain;

public class TransactionValidator {
    public final static String BAD_KEY_MESSAGE = "That key was BAD";
    private final String BAD_KEY = "bad";

    public boolean isKeyBad(String key) {
        // TODO need a better way to associate the validation error with the relevant error message
        return BAD_KEY.equals(key);
    }
}
