package com.jay.mykafka.common;

/**
 * jie.zhou
 * 2018/10/28 9:56
 */
public class UnableToConnectBrokerException extends RuntimeException {
    public UnableToConnectBrokerException(String message) {
        super(message);
    }
}
