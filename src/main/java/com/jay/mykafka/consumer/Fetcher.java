package com.jay.mykafka.consumer;

import com.jay.mykafka.cluster.Broker;
import com.jay.mykafka.cluster.Cluster;
import org.I0Itec.zkclient.ZkClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The fetcher is a background thread that fetches data from a set of servers
 * jie.zhou
 * 2018/11/6 10:16
 */
public class Fetcher {
    private ConsumerConfig config;
    private ZkClient zkClient;
    private FetchRunnable[] fetchThreads;

    public Fetcher(ConsumerConfig config, ZkClient zkClient) {
        this.config = config;
        this.zkClient = zkClient;
    }

    public void startConnections(Cluster cluster, List<PartitionTopicInfo> allPartitionInfo) {
        if (allPartitionInfo == null || allPartitionInfo.size() == 0) {
            return;
        }
        Map<Broker, List<PartitionTopicInfo>> partitionInfoPerBroker = new HashMap<>();
        allPartitionInfo.forEach(partInfo -> {
            Broker broker = cluster.getBroker(partInfo.getBrokerId());
            partitionInfoPerBroker.computeIfAbsent(broker, k -> new ArrayList<>())
                                    .add(partInfo);
        });

        int runnableSize = partitionInfoPerBroker.keySet().size();
        fetchThreads = new FetchRunnable[runnableSize];

        int i = 0;
        for (Map.Entry<Broker, List<PartitionTopicInfo>> entry : partitionInfoPerBroker.entrySet()) {
            Broker broker = entry.getKey();
            List<PartitionTopicInfo> partInfos = entry.getValue();
            FetchRunnable runnable = new FetchRunnable("fetch-runnable-" + i, config, zkClient, broker, partInfos);
            runnable.start();
            i += 1;
        }
    }

    public void stopConnections() {
        for (FetchRunnable fetchThread : fetchThreads) {
            fetchThread.shutdown();
        }
        fetchThreads = null;
    }
}
