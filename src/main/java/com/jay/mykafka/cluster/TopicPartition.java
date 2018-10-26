package com.jay.mykafka.cluster;

/**
 * jie.zhou
 * 2018/10/25 15:26
 */
public class TopicPartition {
    private String topic;
    private int partition;

    public TopicPartition(String topic, String partition) {
        this.topic = topic;
        this.partition = Integer.parseInt(partition);
    }

    public TopicPartition(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }
}
