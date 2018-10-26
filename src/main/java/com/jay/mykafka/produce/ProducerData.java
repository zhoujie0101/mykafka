package com.jay.mykafka.produce;

import java.util.Collections;
import java.util.List;

/**
 * jie.zhou
 * 2018/10/26 15:24
 */
public class ProducerData<K, V> {
    private String topic;
    private K key;
    private List<V> value;

    public ProducerData(String topic, V data) {
        this(topic, Collections.singletonList(data));
    }

    public ProducerData(String topic, List<V> value) {
        this.topic = topic;
        this.value = value;
    }

    public ProducerData(String topic, K key, List<V> value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public K getKey() {
        return key;
    }

    public List<V> getValue() {
        return value;
    }
}
