package com.jay.mykafka.common;

/**
 * jie.zhou
 * 2018/10/26 21:35
 */
public class UnavailableProducerException extends RuntimeException {
    public UnavailableProducerException(String message) {
        super(message);
    }
}
