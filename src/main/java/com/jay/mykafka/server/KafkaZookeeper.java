package com.jay.mykafka.server;

import com.jay.mykafka.cluster.Broker;
import com.jay.mykafka.conf.KafkaConfig;
import com.jay.mykafka.log.LogManager;
import com.jay.mykafka.util.ZKStringSerializer;
import com.jay.mykafka.util.ZKUtils;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * kafka zk管理
 * jie.zhou
 * 2018/10/25 15:58
 */
public class KafkaZookeeper {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaZookeeper.class);

    private KafkaConfig config;
    private LogManager logManager;

    private String brokerIdPath;
    private ZkClient zkClient;
    private List<String> topics = new ArrayList<>();
    private Object lock = new Object();

    public KafkaZookeeper(KafkaConfig config, LogManager logManager) {
        this.config = config;
        this.logManager = logManager;

        brokerIdPath = ZKUtils.BROKER_IDS_PATH + "/" + config.getBrokerId();
    }

    public void startup() {
        LOGGER.info("connecting to ZK: " + config.getZkConnect());

        zkClient = new ZkClient(config.getZkConnect(), config.getZkSessionTimeoutMs(),
                config.getZkConnectionTimeoutMs(), new ZKStringSerializer());
        zkClient.subscribeStateChanges(new SessionExpireListener());
    }

    private class SessionExpireListener implements IZkStateListener {
        @Override
        public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {

        }

        @Override
        public void handleNewSession() throws Exception {
            LOGGER.info("re-registering broker info in ZK for broker " + config.getBrokerId());
            registerBrokerInZK();
            synchronized (lock) {
                LOGGER.info("re-registering broker topics in ZK for broker " + config.getBrokerId());
                topics.forEach(KafkaZookeeper.this::registerTopicInZKInternal);
            }
        }

        @Override
        public void handleSessionEstablishmentError(Throwable error) throws Exception {

        }
    }

    /**
     * register topic in zk
     * @param topic
     */
    public void registerTopicInZK(String topic) {
        registerTopicInZKInternal(topic);
        synchronized (lock) {
            topics.add(topic);
        }
    }

    private void registerTopicInZKInternal(String topic) {
        String brokerTopicPath = ZKUtils.BROKER_TOPICS_PATH + "/" + topic + "/" + config.getBrokerId();
        Integer numPartitions = logManager.getTopicPartitionsMap().getOrDefault(topic, config.getNumPartitions());
        LOGGER.info("Begin registering broker topic " + brokerTopicPath + " with " + numPartitions + " partitions");
        ZKUtils.createEphemeralPathExpectConflict(zkClient, brokerTopicPath, numPartitions.toString());
        LOGGER.info("End registering broker topic " + brokerTopicPath);
    }

    /**
     * register broker in zk
     */
    public void registerBrokerInZK() {
        LOGGER.info("Registering broker " + brokerIdPath);

        String hostname;
        if (config.getHostname() == null) {
            try {
                hostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                throw new RuntimeException("unknown host exception", e);
            }
        } else {
            hostname = config.getHostname();
        }
        Broker broker = new Broker(config.getBrokerId(), brokerIdPath, hostname, config.getPort());
        try {
            ZKUtils.createEphemeralPathExpectConflict(zkClient, brokerIdPath, broker.getZKString());
        } catch (ZkNodeExistsException e) {
            throw new RuntimeException("A broker is already registered on the path " + brokerIdPath + ". This probably " +
                    "indicates that you either have configured a brokerid that is already in use, or " +
                    "else you have shutdown this broker and restarted it faster than the zookeeper " +
                    "timeout so it appears to be re-registering.");
        }

        LOGGER.info("Registering broker " + brokerIdPath + " successed with " + broker);
    }
}
