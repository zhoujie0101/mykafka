package com.jay.mykafka.produce.async;

/**
 * jie.zhou
 * 2018/11/5 14:54
 */
public class QueueItem<T> {
    private String topic;
    private int partition;
    private T event;

    public QueueItem(String topic, int partition, T event) {
        this.topic = topic;
        this.partition = partition;
        this.event = event;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public T getEvent() {
        return event;
    }

    @Override
    public String toString() {
        return "QueueItem{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", event=" + event +
                '}';
    }
}
