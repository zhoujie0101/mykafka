package com.jay.mykafka.consumer;

import com.jay.mykafka.util.ZKUtils;

/**
 * jie.zhou
 * 2018/11/6 10:47
 */
public class ZKGroupDir {
    protected String group;
    protected String consumerDir;
    protected String consumerGroupDir;
    protected String consumerRegistryDir;

    public ZKGroupDir(String group) {
        this.group = group;
        consumerDir = ZKUtils.CONSUMERS_PATH;
        consumerGroupDir = consumerDir + "/" + group;
        consumerRegistryDir = consumerGroupDir + "/ids";
    }

    public String getGroup() {
        return group;
    }

    public String getConsumerDir() {
        return consumerDir;
    }

    public String getConsumerGroupDir() {
        return consumerGroupDir;
    }

    public String getConsumerRegistryDir() {
        return consumerRegistryDir;
    }
}
