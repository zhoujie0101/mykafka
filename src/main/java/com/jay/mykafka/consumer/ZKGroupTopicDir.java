package com.jay.mykafka.consumer;

/**
 * jie.zhou
 * 2018/11/6 10:49
 */
public class ZKGroupTopicDir extends ZKGroupDir {
    private String consumerOffsetDir;
    private String consumerOwnerDir;

    public ZKGroupTopicDir(String group, String topic) {
        super(group);
        consumerOffsetDir = consumerGroupDir + "/offsets/" + topic;
        consumerOwnerDir = consumerGroupDir + "/owners/" + topic;
    }

    public String getConsumerOffsetDir() {
        return consumerOffsetDir;
    }

    public String getConsumerOwnerDir() {
        return consumerOwnerDir;
    }
}
