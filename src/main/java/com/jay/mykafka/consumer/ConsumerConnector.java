package com.jay.mykafka.consumer;

import com.jay.mykafka.serializer.Decoder;

import java.util.List;
import java.util.Map;

/**
 * Main interface for consumer
 * jie.zhou
 * 2018/11/5 18:37
 */
public interface ConsumerConnector {
    /**
     *  Create a list of MessageStreams for each topic.
     *
     *  @param topicCountMap  a map of (topic, #streams) pair
     *  @return a map of (topic, list of  KafkaStream) pairs.
     *          The number of items in the list is #streams. Each stream supports
     *          an iterator over message/metadata pairs.
     */
    <T> Map<String, List<KafkaStream<T>>> createMessageStreams(Map<String, Integer> topicCountMap);

    /**
     *  Create a list of MessageStreams for each topic.
     *
     *  @param topicCountMap  a map of (topic, #streams) pair
     *  @param decoder Decoder to decode each Message to type T
     *  @return a map of (topic, list of  KafkaStream) pairs.
     *          The number of items in the list is #streams. Each stream supports
     *          an iterator over message/metadata pairs.
     */
    <T> Map<String, List<KafkaStream<T>>> createMessageStreams(Map<String, Integer> topicCountMap, Decoder<T> decoder);

    void commitOffsets();

    void shutdown();
}
