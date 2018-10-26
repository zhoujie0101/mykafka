package com.jay.mykafka.produce;

/**
 * jie.zhou
 * 2018/10/26 10:44
 */
public interface Partitioner<K> {
    int partition(K key, int numPartitions);
}
