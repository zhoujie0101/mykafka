package com.jay.mykafka.produce;

import com.jay.mykafka.cluster.Broker;
import com.jay.mykafka.cluster.Partition;
import com.jay.mykafka.conf.ZKConfig;
import com.jay.mykafka.util.ZKStringSerializer;
import com.jay.mykafka.util.ZKUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * jie.zhou
 * 2018/10/26 10:51
 */
public class ZKBrokerPartitionInfo implements BrokerPartitionInfo {
    private Object zkWatcherLock = new Object();
    private ZKConfig config;
    private ZkClient zkClient;
    //topic -> partitions
    private Map<String, SortedSet<Partition>> topicBrokerPartitions;
    //brokerId -> broker
    Map<Integer, Broker> allBrokers;

    private BrokerTopicListener brokerTopicListener;

    public ZKBrokerPartitionInfo(ZKConfig config) {
        this.config = config;
        this.zkClient = new ZkClient(config.getZkConnect(), config.getZkSessionTimeoutMs(),
                config.getZkConnectionTimeoutMs(), new ZKStringSerializer());

        allBrokers = getZKBrokerInfo();

        topicBrokerPartitions = getZKTopicPartitionInfo();

        brokerTopicListener = new BrokerTopicListener(topicBrokerPartitions, allBrokers);
        zkClient.subscribeChildChanges(ZKUtils.BROKER_TOPICS_PATH, brokerTopicListener);
        topicBrokerPartitions.keySet().forEach(topic ->
                zkClient.subscribeChildChanges(ZKUtils.BROKER_TOPICS_PATH + "/" + topic, brokerTopicListener));
        zkClient.subscribeChildChanges(ZKUtils.BROKER_IDS_PATH, brokerTopicListener);
    }

    private Map<Integer, Broker> getZKBrokerInfo() {
        Map<Integer, Broker> brokers = new HashMap<>();
        List<String> allBrokerIds = zkClient.getChildren(ZKUtils.BROKER_IDS_PATH);
        allBrokerIds.forEach(brokerId -> {
            int bid = Integer.parseInt(brokerId);
            String brokerInfo = zkClient.readData(ZKUtils.BROKER_IDS_PATH + "/" + brokerId);
            brokers.put(bid, Broker.create(bid, brokerInfo));
        });

        return brokers;
    }

    private Map<String, SortedSet<Partition>> getZKTopicPartitionInfo() {
        Map<String, SortedSet<Partition>> brokerPartitionsPerTopic = new HashMap<>();

        ZKUtils.makeSurePersistentPathExists(zkClient, ZKUtils.BROKER_TOPICS_PATH);
        List<String> topics = zkClient.getChildren(ZKUtils.BROKER_TOPICS_PATH);
        if (topics != null && !topics.isEmpty()) {
            topics.forEach(topic -> {
                String brokerTopicPath = ZKUtils.BROKER_TOPICS_PATH + "/" + topic;
                List<String> brokerList = zkClient.getChildren(brokerTopicPath);
                if (brokerList != null && !brokerList.isEmpty()) {
                    SortedSet<Partition> brokerPartitions = new TreeSet<>();
                    brokerList.forEach( brokerId -> {
                        String numPartitions = zkClient.readData(brokerTopicPath + "/" + brokerId);
                        Partition partition = new Partition(Integer.valueOf(brokerId), Integer.parseInt(numPartitions));
                        brokerPartitions.add(partition);
                    });
                    brokerPartitionsPerTopic.put(topic, brokerPartitions);
                }
            });
        }

        return brokerPartitionsPerTopic;
    }

    @Override
    public SortedSet<Partition> getBrokerPartitionInfo(String topic) {
        synchronized (zkWatcherLock) {
            SortedSet<Partition> partitions = topicBrokerPartitions.get(topic);
            if (partitions == null || partitions.isEmpty()) {
                partitions = bootstrapWithExistingBrokers(topic);
                topicBrokerPartitions.put(topic, partitions);
            }
            return partitions;
        }
    }

    private SortedSet<Partition> bootstrapWithExistingBrokers(String topic) {
        List<String> allBrokerIds = zkClient.getChildren(ZKUtils.BROKER_IDS_PATH);

        SortedSet<Partition> partitions = new TreeSet<>();
        allBrokerIds.forEach(brokerId -> partitions.add(new Partition(Integer.parseInt(brokerId), 0)));

        return partitions;
    }

    @Override
    public Broker getBrokerInfo(int brokerId) {
        return allBrokers.get(brokerId);
    }

    @Override
    public Map<Integer, Broker> getAllBrokerInfo() {
        return allBrokers;
    }

    @Override
    public void updateInfo() {
        synchronized (zkWatcherLock) {
            topicBrokerPartitions = getZKTopicPartitionInfo();
            allBrokers = getZKBrokerInfo();
        }
    }

    @Override
    public void close() {
        zkClient.close();
    }

    private class BrokerTopicListener implements IZkChildListener {
        public BrokerTopicListener(Map<String, SortedSet<Partition>> topicBrokerPartitions,
                                   Map<Integer, Broker> allBrokers) {

        }

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {

        }
    }
}
