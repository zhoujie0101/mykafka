package com.jay.mykafka.produce;

import com.jay.mykafka.cluster.Broker;
import com.jay.mykafka.cluster.Partition;
import com.jay.mykafka.common.InvalidPartitionException;
import com.jay.mykafka.common.NoBrokersForPartitionException;
import com.jay.mykafka.conf.ZKConfig;
import com.jay.mykafka.produce.async.CallbackHandler;
import com.jay.mykafka.produce.async.EventHandler;
import com.jay.mykafka.serializer.Encoder;
import com.jay.mykafka.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * jie.zhou
 * 2018/10/26 10:43
 */
public class Producer<K, V> {
    public static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    private ProducerConfig config;
    private Partitioner<K> partitioner;
    private ProducerPool<V> producerPool;
    private BrokerPartitionInfo brokerPartitionInfo;

    private Random rand = new Random();

    public Producer(ProducerConfig config) {
        this(config, Utils.getObject(config.getPartitionerClass()),
                new ProducerPool<>(config, Utils.getObject(config.getAsyncProducerConfigShared().getSerializerClass()))
        );
    }

    private Producer(ProducerConfig config, Partitioner<K> partitioner, ProducerPool<V> producerPool) {
        this.config = config;
        if (partitioner == null) {
            this.partitioner = new DefaultPartitioner<>();
        } else {
            this.partitioner = partitioner;
        }
        this.producerPool = producerPool;
        boolean zkEnable = config.getZkConnect() != null && config.getZkConnect().length() > 0;
        if (zkEnable) {
            Properties zkProps = new Properties();
            zkProps.put("zk.connect", config.getZkConnect());
            zkProps.put("zk.sessiontimeout.ms", config.getZkSessionTimeoutMs());
            zkProps.put("zk.connectiontimeout.ms", config.getSyncProducerConfig().getConnectTimeoutMs());
            zkProps.put("zk.synctime.ms", config.getZkSyncTimeMs());
            brokerPartitionInfo = new ZKBrokerPartitionInfo(new ZKConfig(zkProps));
        }
    }

    public Producer(ProducerConfig config, Encoder<V> serializer, EventHandler<V> eventHandler,
                    CallbackHandler<V> ckHandler, Partitioner<K> partitioner) {
        this(config, partitioner, new ProducerPool<>(config, serializer, eventHandler, ckHandler));
    }

    /**
     * Sends the data, partitioned by key to the topic using either the
     * synchronous or the asynchronous producer
     * @param data the producer data object that encapsulates the topic, key and message data
     */
    public void send(ProducerData<K, V> data) {
        send(Collections.singletonList(data));
    }

    /**
     * Sends the data, partitioned by key to the topic using either the
     * synchronous or the asynchronous producer
     * @param dataList the producer data object that encapsulates the topic, key and message data
     */
    public void send(List<ProducerData<K, V>> dataList) {
        List<ProducerPool<V>.ProducePoolData<V>> producePoolRequests = dataList.stream().map(data -> {
            Partition selectedPartition = null;
            Broker selectedBroker = null;

            int numRetries = 0;
            while (numRetries <= config.getNumRetries() && selectedBroker == null) {
                if (numRetries > 0) {
                    LOGGER.info("Try #" + numRetries
                            + " ZK produce cache is stale. Refreshing it by reading from ZK again");
                    brokerPartitionInfo.updateInfo();
                }
                List<Partition> partitionList = new ArrayList<>(
                        brokerPartitionInfo.getBrokerPartitionInfo(data.getTopic()));
                if (partitionList.isEmpty()) {
                    throw new NoBrokersForPartitionException("Partition = " + data.getTopic());
                }
                int partitionId = getPartition(data.getKey(), partitionList.size());
                selectedPartition = partitionList.get(partitionId);
                selectedBroker = brokerPartitionInfo.getBrokerInfo(selectedPartition.getBrokerId());

                numRetries++;
            }

            if (selectedBroker == null) {
                throw new NoBrokersForPartitionException("Invalid Zookeeper state. Failed to get partition for topic: " +
                        data.getTopic() + " and key: " + data.getKey());
            }

            return producerPool.createProducePoolData(data.getTopic(), new Partition(selectedPartition.getBrokerId(),
                    selectedPartition.getPartitionId()), data.getValue());

        }).collect(Collectors.toList());

        producerPool.send(producePoolRequests);
    }

    private int getPartition(K key, int numPartitions) {
        if (numPartitions <= 0) {
            throw new InvalidPartitionException("Invalid number of partitions: " + numPartitions
                    + "\n Valid values are > 0");
        }

        int partition;
        if (key == null) {
            partition = rand.nextInt(numPartitions);
        } else {
            partition = partitioner.partition(key, numPartitions);
        }

        if (partition < 0 || partition >= numPartitions) {
            throw new InvalidPartitionException("Invalid partitions id: " + partition
                    + "\n Valid values are in the range inclusive [0, " + (numPartitions - 1) + "]");
        }
        return 0;
    }
}
