package com.jay.mykafka.consumer;

import com.jay.mykafka.cluster.Broker;
import com.sun.xml.internal.bind.v2.TODO;
import org.I0Itec.zkclient.ZkClient;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * jie.zhou
 * 2018/11/6 21:27
 */
public class FetchRunnable extends Thread {
    private String name;
    private ConsumerConfig config;
    private ZkClient zkClient;
    private Broker broker;
    private List<PartitionTopicInfo> partInfos;
    private volatile boolean stopped = false;
    private CountDownLatch shutdownLatch = new CountDownLatch(1);

    public FetchRunnable(String name, ConsumerConfig config, ZkClient zkClient, Broker broker,
                         List<PartitionTopicInfo> partInfos) {
        super(name);
        this.name = name;
        this.config = config;
        this.zkClient = zkClient;
        this.broker = broker;
        this.partInfos = partInfos;
    }

    @Override
    public void run() {
        while (!stopped) {
            //TODO implement fetch
        }
    }

    public void shutdown() {
        stopped = true;
        interrupt();
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
