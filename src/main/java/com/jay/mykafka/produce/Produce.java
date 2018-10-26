package com.jay.mykafka.produce;

/**
 * jie.zhou
 * 2018/10/26 10:43
 */
public class Produce<K, V> {
    private ProduceConfig config;
    private Partitioner<K> partitioner;
}
