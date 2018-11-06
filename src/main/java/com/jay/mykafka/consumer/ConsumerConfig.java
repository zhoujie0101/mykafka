package com.jay.mykafka.consumer;

import com.jay.mykafka.api.OffsetRequest;
import com.jay.mykafka.conf.ZKConfig;
import com.jay.mykafka.util.Utils;

import java.util.Properties;

/**
 * jie.zhou
 * 2018/11/5 18:40
 */
public class ConsumerConfig extends ZKConfig {

    /**
     * a string that uniquely identifies a set of consumers within the same consumer group
     */
    private String groupId;
    /**
     * consumer id: generated automatically if not set. Set this explicitly for only testing purpose.
     */
    private String consumerId;
    /**
     * the socket timeout for network requests
     */
    private int socketTimeoutMs;
    /**
     * the socket receive buffer for network requests
     */
    private int sockerBufferSize;
    /**
     * the number of byes of messages to attempt to fetch
     */
    private int fetchSize;
    /**
     * to avoid repeatedly polling a broker node which has no new data
     * we will backoff every time we get an empty set from the broker
     */
    private int fetcherBackoffMs;
    /**
     * if true, periodically commit to zookeeper the offset of messages already fetched by the consumer
     */
    private boolean autoCommit;

    /**
     * the frequency in ms that the consumer offsets are committed to zookeeper
     */
    private int autoCommitIntervalMs;

    /**
     * max number of messages buffered for consumption
     */
    private int maxQueuedChunks;

    /**
     * max number of retries during rebalance
     */
    private int maxRebalanceRetries;
    /**
     * backoff time between retries during rebalance
     */
    private int rebalanceBackoffMs;

    /**
     * what to do if an offset is out of range.
     * smallest : automatically reset the offset to the smallest offset
     * largest : automatically reset the offset to the largest offset
     * anything else: throw exception to the consumer
     */
    private String autoOffsetReset;
    /**
     * throw a timeout exception to the consumer if no message is available for consumption after the specified interval
     */
    private int consumerTimeoutMs;
    /**
     * Use shallow iterator over compressed messages directly. This feature should be used very carefully.
     * Typically, it's only used for mirroring raw messages from one kafka cluster to another to save the
     * overhead of decompression.
     */
    private boolean enableShallowIterator;

    public ConsumerConfig(Properties props) {
        super(props);

        this.groupId = Utils.getString(props, "groupid");
        this.consumerId = Utils.getString(props, "consumerid");
        this.socketTimeoutMs = Utils.getInt(props, "socker.timeout.size", 30 * 1000);
        this.sockerBufferSize = Utils.getInt(props, "socket.buffer.size", 64 * 1024);
        this.fetchSize = Utils.getInt(props, "fetch.size", 1024 * 1024);
        this.fetcherBackoffMs = Utils.getInt(props, "fetcher.backoff.ms", 1000);
        this.autoCommit = Utils.getBoolean(props, "autocommit.enable", true);
        this.autoCommitIntervalMs = Utils.getInt(props, "autocommit.interval.ms", 10 * 1000);
        this.maxQueuedChunks = Utils.getInt(props, "queuechunks.max", 10);
        this.maxRebalanceRetries = Utils.getInt(props, "rebalance.retries.max", 4);
        this.rebalanceBackoffMs = Utils.getInt(props, "rebalance.backoff.ms", getZkSyncTimeMs());
        this.autoOffsetReset = Utils.getString(props, "autooffset.reset", OffsetRequest.SMALLEST_TIME);
        this.consumerTimeoutMs = Utils.getInt(props, "consumer.timeout.ms", -1);
        this.enableShallowIterator = Utils.getBoolean(props, "shallowiterator.enable", false);
    }

    public String getGroupId() {
        return groupId;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    public int getSockerBufferSize() {
        return sockerBufferSize;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public int getFetcherBackoffMs() {
        return fetcherBackoffMs;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public int getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    public int getMaxQueuedChunks() {
        return maxQueuedChunks;
    }

    public int getMaxRebalanceRetries() {
        return maxRebalanceRetries;
    }

    public int getRebalanceBackoffMs() {
        return rebalanceBackoffMs;
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public int getConsumerTimeoutMs() {
        return consumerTimeoutMs;
    }

    public boolean isEnableShallowIterator() {
        return enableShallowIterator;
    }
}
