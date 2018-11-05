package com.jay.mykafka.common;

/**
 * jie.zhou
 * 2018/11/5 14:52
 */
public class QueueClosedException extends RuntimeException {
    public QueueClosedException(String message) {
        super(message);
    }
}
