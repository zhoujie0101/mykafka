package com.jay.mykafka.common;

/**
 * jie.zhou
 * 2018/10/26 20:56
 */
public class InvalidMessageSizeException extends RuntimeException {
    public InvalidMessageSizeException(String message) {
        super(message);
    }
}
