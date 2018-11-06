package com.jay.mykafka.consumer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * jie.zhou
 * 2018/11/6 18:07
 */
public class PartitionTopicInfo {
    private String topic;
    private int brokerId;
    private int partition;
    private BlockingQueue<FetchedDataChunk> queue;
    private AtomicLong consumedOffset;
    private AtomicLong fetchedOffset;
    private AtomicInteger fetchSize;

    public PartitionTopicInfo(String topic, int brokerId, int partition, BlockingQueue<FetchedDataChunk> queue,
                              AtomicLong consumedOffset, AtomicLong fetchedOffset, AtomicInteger fetchSize) {
        this.topic = topic;
        this.brokerId = brokerId;
        this.partition = partition;
        this.queue = queue;
        this.consumedOffset = consumedOffset;
        this.fetchedOffset = fetchedOffset;
        this.fetchSize = fetchSize;
    }

    public void resetConsumeOffset(long newConsumeOffset) {
        consumedOffset.set(newConsumeOffset);
    }

    public void resetFetchOffset(long newFetchOffset) {
        fetchedOffset.set(newFetchOffset);
    }

    public long getConsumedOffset() {
        return consumedOffset.get();
    }

    public long getFetchedOffset() {
        return fetchedOffset.get();
    }

    public String getTopic() {
        return topic;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public int getPartition() {
        return partition;
    }
}
