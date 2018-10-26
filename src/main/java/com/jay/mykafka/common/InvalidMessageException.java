package com.jay.mykafka.common;

/**
 * jie.zhou
 * 2018/10/26 20:38
 */
public class InvalidMessageException extends RuntimeException {
    public InvalidMessageException(String message) {
        super(message);
    }
}
