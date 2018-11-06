package com.jay.mykafka.produce.async;

/**
 * jie.zhou
 * 2018/11/5 15:30
 */
public class IllegalQueueStateException extends RuntimeException {
    public IllegalQueueStateException(String message) {
        super(message);
    }
}
