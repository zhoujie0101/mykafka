package com.jay.mykafka.produce;

import java.util.Random;

/**
 * jie.zhou
 * 2018/10/26 10:44
 */
public class DefaultPartitioner<K> implements Partitioner<K> {
    private Random rand = new Random();

    @Override
    public int partition(K key, int numPartitions) {
        if (key == null) {
            return rand.nextInt(numPartitions);
        }

        return Math.abs(key.hashCode()) % numPartitions;
    }
}
