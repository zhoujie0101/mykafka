package com.jay.mykafka.produce;

import com.jay.mykafka.conf.ZKConfig;
import com.jay.mykafka.produce.async.AsyncProducerConfig;
import com.jay.mykafka.produce.async.AsyncProducerConfigShared;
import com.jay.mykafka.util.Utils;

import java.util.Properties;

/**
 * jie.zhou
 * 2018/10/26 10:24
 */
public class ProducerConfig extends ZKConfig {
    /**
     * If DefaultEventHandler is used, this specifies the number of times to
     * retry if an error is encountered during send. Currently, it is only
     * appropriate when broker.list points to a VIP. If the zk.connect option
     * is used instead, this will not have any effect because with the zk-based
     * producer, brokers are not re-selected upon retry. So retries would go to
     * the same (potentially still down) broker. (KAFKA-253 will help address
     * this.)
     */
    private int numRetries;
    /**
     * the partitioner class for partitioning events amongst sub-topics
     */
    private String partitionerClass;
    /** this parameter specifies whether the messages are sent asynchronously or not.
     * Valid values are - async for asynchronous send
     *                    sync for synchronous send
     */
     private String produceType;
    /**
     * The producer using the zookeeper software load balancer maintains a ZK cache that gets
     * updated by the zookeeper watcher listeners. During some events like a broker bounce, the
     * producer ZK cache can get into an inconsistent state, for a small time period. In this time
     * period, it could end up picking a broker partition that is unavailable. When this happens, the
     * ZK cache needs to be updated.
     * This parameter specifies the number of times the producer attempts to refresh this ZK cache.
     */
     private int zkReadRetries;

     private SyncProducerConfig syncProducerConfig;

     private AsyncProducerConfigShared asyncProducerConfigShared;

    public ProducerConfig(Properties props) {
        super(props);

        syncProducerConfig = new SyncProducerConfig(props);

        asyncProducerConfigShared = new AsyncProducerConfigShared(props);

        this.numRetries = Utils.getInt(props, "num.retries", 0);
        this.partitionerClass = Utils.getString(props, "partitioner.class",
                "com.jay.mykafka.produce.DefaultPartitioner");
        this.produceType = Utils.getString(props, "product.type", "sync");
        this.zkReadRetries = Utils.getInt(props, "zk.read.num.retries", 3);
    }

    public int getNumRetries() {
        return numRetries;
    }

    public String getPartitionerClass() {
        return partitionerClass;
    }

    public String getProduceType() {
        return produceType;
    }

    public int getZkReadRetries() {
        return zkReadRetries;
    }

    public SyncProducerConfig getSyncProducerConfig() {
        return syncProducerConfig;
    }

    public AsyncProducerConfigShared getAsyncProducerConfigShared() {
        return asyncProducerConfigShared;
    }
}
