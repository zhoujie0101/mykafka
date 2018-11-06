package com.jay.mykafka.consumer;

import java.util.Map;
import java.util.Set;

/**
 * jie.zhou
 * 2018/11/6 10:42
 */
public class StaticTopicCount implements TopicCount {

    private String consumerIdString;
    private Map<String, Integer> topicCountMap;

    public StaticTopicCount(String consumerIdString, Map<String, Integer> topicCountMap) {
        this.consumerIdString = consumerIdString;
        this.topicCountMap = topicCountMap;
    }

    @Override
    public Map<String, Set<String>> getConsumerThreadIsPerTopic() {
        return makeConsumerThreadIsPerTopic(consumerIdString, topicCountMap);
    }

    /**
     *  return json of
     *  { "topic1" : 4,
     *    "topic2" : 4
     *  }
     */
    @Override
    public String dbString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        int i = 0;
        for (Map.Entry<String, Integer> entry : topicCountMap.entrySet()) {
            String topic = entry.getKey();
            int nConsumers = entry.getValue();
            if (i > 0) {
                builder.append(",");
            }
            builder.append("\"").append(topic).append("\":").append(nConsumers);
            i++;
        }
        builder.append("}");

        return builder.toString();
    }
}
