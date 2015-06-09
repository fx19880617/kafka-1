package com.uber.kafka.tools;

public enum ErrorCode {
    NA(0),
    CONSUMER_TIMEOUT(1),
    UNKNOWN(2),
    ;

    private final int value;

    private ErrorCode(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

}
