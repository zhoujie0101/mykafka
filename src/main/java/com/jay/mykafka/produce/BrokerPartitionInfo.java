package com.jay.mykafka.produce;

import com.jay.mykafka.cluster.Broker;
import com.jay.mykafka.cluster.Partition;

import java.util.Map;
import java.util.SortedSet;

/**
 * jie.zhou
 * 2018/10/26 10:46
 */
public interface BrokerPartitionInfo {
    SortedSet<Partition> getBrokerPartitionInfo(String topic);

    Broker getBrokerInfo(int brokerId);

    Map<Integer, Broker> getAllBrokerInfo();

    void updateInfo();

    void close();
}
