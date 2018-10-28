package com.jay.mykafka.common;

/**
 * jie.zhou
 * 2018/10/28 10:49
 */
public class InvalidRequestException extends RuntimeException {
    public InvalidRequestException(String message) {
        super(message);
    }
}
