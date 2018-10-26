package com.jay.mykafka.common;

/**
 * jie.zhou
 * 2018/10/26 15:41
 */
public class NoBrokersForPartitionException extends RuntimeException {
    public NoBrokersForPartitionException(String message) {
        super(message);
    }
}
