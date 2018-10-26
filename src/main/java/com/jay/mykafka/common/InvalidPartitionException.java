package com.jay.mykafka.common;

/**
 * jie.zhou
 * 2018/10/26 15:44
 */
public class InvalidPartitionException extends RuntimeException {
    public InvalidPartitionException(String message) {
        super(message);
    }
}
