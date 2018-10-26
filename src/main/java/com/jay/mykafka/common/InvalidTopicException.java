package com.jay.mykafka.common;

/**
 * jie.zhou
 * 2018/10/26 20:54
 */
public class InvalidTopicException extends RuntimeException {
    public InvalidTopicException(String message) {
        super(message);
    }
}
