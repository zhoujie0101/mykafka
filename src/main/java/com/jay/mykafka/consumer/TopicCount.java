package com.jay.mykafka.consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.I0Itec.zkclient.ZkClient;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * jie.zhou
 * 2018/11/6 10:39
 */
public interface TopicCount {
    Map<String, Set<String>> getConsumerThreadIsPerTopic();

    String dbString();

    static TopicCount constructStatic(String group, String consumerId, ZkClient zkClient) {
        ZKGroupDir dir = new ZKGroupDir(group);
        String json = zkClient.readData(dir.getConsumerRegistryDir() + "/" + consumerId);
        return constructStatic(consumerId, json);
    }

    static TopicCount constructStatic(String consumerId, String json) {
        Map<String, Integer> topicCountMap = JSON.parseObject(json, new TypeReference<Map<String, Integer>>(){});
        return new StaticTopicCount(consumerId, topicCountMap);
    }

    default Map<String, Set<String>> makeConsumerThreadIsPerTopic(String consumerIdString,
                                                                  Map<String, Integer> topicCountMap) {
        Map<String, Set<String>> consumerThreadIsPerTopic = new HashMap<>(topicCountMap.size());

        topicCountMap.forEach((topic, nConsumers) -> {
            Set<String> consumerSet = new HashSet<>(nConsumers);
            for (int i = 0; i < nConsumers; i++) {
                consumerSet.add(consumerIdString + "-" + i);
            }
            consumerThreadIsPerTopic.put(topic, consumerSet);
        });

        return consumerThreadIsPerTopic;
    }
}
