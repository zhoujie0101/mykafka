package com.jay.mykafka.util;

import com.jay.mykafka.cluster.Broker;
import com.jay.mykafka.cluster.Cluster;
import com.jay.mykafka.consumer.TopicCount;
import com.jay.mykafka.consumer.ZKGroupDir;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * jie.zhou
 * 2018/10/25 15:46
 */
public class ZKUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKUtils.class);

    public static final String CONSUMERS_PATH = "/consumers";
    public static final String BROKER_IDS_PATH = "/brokers/ids";
    public static final String BROKER_TOPICS_PATH = "/brokers/topics";

    public static void makeSurePersistentPathExists(ZkClient client, String path) {
        if (!client.exists(path)) {
            client.createPersistent(path, true);
        }
    }

    public static void createEphemeralPathExpectConflict(ZkClient client, String path, String data) {
        try {
            createEphemeralPath(client, path, data);
        } catch (ZkNodeExistsException e) {
            // this can happen when there is connection loss; make sure the data is what we intend to write
            try {
                String storedData = client.readData(path);
                if (storedData == null || !storedData.equals(data)) {
                    LOGGER.info("conflict in " + path + " data: " + data + " stored data: " + storedData);
                    throw e;
                } else {
                    LOGGER.info(path + " exists with value " + data + " during connection loss; this is ok");
                }
            } catch (ZkNoNodeException e1) {
                // the node disappeared; treat as if node existed and let caller handles this
            } catch (Exception e2) {
                throw e2;
            }
        } catch (Exception e2) {
            throw e2;
        }
    }

    private static void createEphemeralPath(ZkClient client, String path, String data) {
        try {
            client.createEphemeral(path, data);
        } catch (ZkNoNodeException e) {
            createParentPath(client, path);
            client.createEphemeral(path, data);
        }
    }

    private static void createParentPath(ZkClient client, String path) {
        String parent = path.substring(0, path.lastIndexOf("/"));
        if (parent.length() != 0) {
            client.createPersistent(parent, true);
        }
    }

    public static Cluster getCluster(ZkClient zkClient) {
        Cluster cluster = new Cluster();
        List<String> brokerIds = zkClient.getChildren(BROKER_IDS_PATH);
        brokerIds.forEach(brokerId -> {
            String brokerZKString = zkClient.readData(BROKER_IDS_PATH + "/" + brokerId);
            cluster.addBroker(Broker.create(Integer.parseInt(brokerId), brokerZKString));
        });

        return cluster;
    }

    /**
     * 获取topic下消费信息
     * @param zkClient
     * @param dir
     * @return topic -> consumerThreadIds
     */
    public static Map<String, List<String>> getConsumersPerTopic(ZkClient zkClient, ZKGroupDir dir) {
        Map<String, List<String>> consumersPerTopic = new HashMap<>();

        List<String> consumers = zkClient.getChildren(dir.getConsumerRegistryDir());
        consumers.forEach(consumer -> {
            String data = zkClient.readData(dir.getConsumerRegistryDir() + "/" + consumer);
            TopicCount tc = TopicCount.constructStatic(consumer, data);
            Map<String, Set<String>> consumerThreadIdsPerTopic = tc.getConsumerThreadIsPerTopic();
            consumerThreadIdsPerTopic.forEach((topic, threadIdSet) ->
                threadIdSet.forEach(threadId ->
                        consumersPerTopic.computeIfAbsent(topic, k -> new ArrayList<>())
                                            .add(threadId))
            );
        });

        Map<String, List<String>> sortedConsumersPerTopic = new HashMap<>();
        consumersPerTopic.forEach((topic, threadIds) -> {
            threadIds.sort(Comparator.naturalOrder());
            sortedConsumersPerTopic.put(topic, threadIds);
        });

        return sortedConsumersPerTopic;
    }

    /**
     * 获取topic下的partition信息
     * @param zkClient
     * @param topics
     * @return  topic -> partitions (brokerId-partitionId)
     */
    public static Map<String, List<String>> getPartitionsPerTopic(ZkClient zkClient, Iterable<String> topics) {
        Map<String, List<String>> partitionsPerTopic = new HashMap<>();
        for (String topic : topics) {
            List<String> partitions = new ArrayList<>();
            List<String> brokerIds = zkClient.getChildren(BROKER_TOPICS_PATH + "/" + topic);
            for (String brokerId : brokerIds) {
                List<String> nParts = zkClient.getChildren(BROKER_TOPICS_PATH + "/" + topic + "/" + brokerId);
                for (int i = 0; i < nParts.size(); i++) {
                    partitions.add(brokerId + "-" + i);
                }
            }
            partitions.sort(Comparator.naturalOrder());
            partitionsPerTopic.put(topic, partitions);
        }

        return partitionsPerTopic;
    }
}
