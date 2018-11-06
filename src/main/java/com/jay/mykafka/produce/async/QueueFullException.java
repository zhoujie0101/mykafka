package com.jay.mykafka.produce.async;

/**
 * jie.zhou
 * 2018/11/5 16:02
 */
public class QueueFullException extends RuntimeException {
    public QueueFullException(String message) {
        super(message);
    }
}
