package com.jay.mykafka.conf;

import com.jay.mykafka.util.Utils;

import javax.rmi.CORBA.Util;
import java.util.Map;
import java.util.Properties;

/**
 * jie.zhou
 * 2018/10/25 09:29
 */
public class KafkaConfig extends ZKConfig {

    /**
     * hostname of broker
     * 默认InetAddress.getLocalHost().getHostAddress()
     */
    private String hostname;
    /**
     * the port to listen and accept connections on
     */
    private int port;
    /**
     * the broker id of this server
     */
    private int brokerId;
    /**
     * SO_SNDBUFF
     */
    private int socketSendBuffer;
    /**
     * SO_RCVBUFF
     */
    private int socketReceiveBuffer;
    /**
     * the maximum number of bytes in a socket request
     * socket request最大字节数
     */
    private int maxSocketRequestSize;
    /**
     * the maximum size of message that the server can receive
     * 服务端能接受的消息体最大尺寸
     */
    private int maxMessageSize;
    /**
     * the number of worker threads that the server uses for handling all client requests
     * 服务端worker线程数，用来处理来自客户端的请求
     */
    private int numThreads;
    /**
     * the interval in which to measure performance statistics
     * 度量性能数据间隔时间，秒
     */
    private int monitoringPeriodSecs;
    /**
     * the default number of log partitions per topic
     * 每个topic的默认分区数
     */
    private int numPartitions;
    /**
     * the directory in which the log data is kept
     * log data目录
     */
    private String logDir;
    /**
     * the maximum size of a single log file
     * 单个log文件最大大小
     */
    private int logFileSize;
    /**
     * the maximum size of a single log file for some specific topic
     * 某些特定topic中单个log文件最大大小
     */
    private Map<String, Integer> logFileSizeMap;
    /**
     * the maximum time before a new log segment is rolled out
     */
    private int logRollHours;
    /**
     * the number of hours before rolling out a new log segment for some specific topic
     */
    private Map<String, Integer> logRollHoursMap;
    /**
     * the number of hours to keep a log file before deleting it
     * log文件保留时间，小时
     */
    private int logRetentionHours;
    /**
     * the number of hours to keep a log file before deleting it for some specific topic
     * 某些特定topic log文件保留时间，小时
     */
    private Map<String, Integer> logRetentionHoursMap;
    /**
     * the maximum size of the log before deleting it
     */
    private long logRetentionSize;
    /**
     * the maximum size of the log for some specific topic before deleting it
     */
    private Map<String, Integer> logRetentionSizeMap;
    /**
     * the maximum size of the log for some specific topic before deleting it
     */
    private int logCleanupIntervalMins;
    /**
     * enable zookeeper registration in the server
     */
    private boolean enableZookeeper;
    /**
     * the number of messages accumulated on a log partition before messages are flushed to disk
     */
    private int flushInterval;
    /**
     * the maximum time in ms that a message in selected topics is kept in memory before flushed to disk
     * e.g., topic1:3000,topic2: 6000
     */
    private Map<String, Integer> flushIntervalMap;
    /**
     * the frequency in ms that the log flusher checks whether any log needs to be flushed to disk
     */
    private int flushSchedulerThreadRate;
    /**
     * the maximum time in ms that a message in any topic is kept in memory before flushed to disk
     */
    private int defaultFlushIntervalMs;
    /**
     * the number of partitions for selected topics, e.g., topic1:8,topic2:16
     */
    private Map<String, Integer> topicPartitionMap;
    /**
     * the maximum length of topic name
     */
    private int maxTopicNameLength;

    public KafkaConfig(Properties props) {
        super(props);
    }

    @Override
    protected void init() {
        super.init();

        this.hostname = Utils.getString(props, "hostname");
        this.port = Utils.getInt(props, "port", 6667);
        this.brokerId = Utils.getInt(props, "brokerid");

        this.socketSendBuffer = Utils.getInt(props, "socket.send.buffer", 100 * 1024);
        this.socketReceiveBuffer = Utils.getInt(props, "socket.receive.buffer", 100 * 1024);
        this.maxSocketRequestSize = Utils.getIntInRange(props, "max.socket.request.bytes", 100 * 1024 * 1024,
                1, Integer.MAX_VALUE);
        this.maxMessageSize = Utils.getIntInRange(props, "max.message.size", 100 * 100, 0, Integer.MAX_VALUE);

        this.numThreads = Utils.getIntInRange(props, "num.threads", Runtime.getRuntime().availableProcessors(),
                1, Integer.MAX_VALUE);
        this.monitoringPeriodSecs = Utils.getIntInRange(props, "monitoring.period.secs", 600, 1, Integer.MAX_VALUE);
        this.numPartitions = Utils.getIntInRange(props, "num.partitions", 1, 1, Integer.MAX_VALUE);
        this.logDir = Utils.getString(props, "log.dir");
        //1G
        this.logFileSize = Utils.getIntInRange(props, "log.file.size", 1024 * 1024 * 1024, 4, Integer.MAX_VALUE);
        this.logFileSizeMap = Utils.getTopicFileSize(Utils.getString(props, "topic.log.file.size", ""));

        this.logRollHours = Utils.getIntInRange(props, "log.roll.hours", 24 * 7, 1, Integer.MAX_VALUE);
        this.logRollHoursMap = Utils.getTopicRollHours(Utils.getString(props, "topic.log.roll.hours", ""));

        this.logRetentionHours = Utils.getIntInRange(props, "log.retention.hours", 24 * 7, 1, Integer.MAX_VALUE);
        this.logRetentionHoursMap = Utils.getTopicRetentionHours(Utils.getString(props, "topic.log.retention.hours", ""));

        this.logRetentionSize = Utils.getLong(props, "log.retention.size", -1);
        this.logRetentionSizeMap = Utils.getTopicRetentionSize(Utils.getString(props, "topic.log.retention.size", ""));

        this.logCleanupIntervalMins = Utils.getIntInRange(props, "log.cleanup.interval.min", 10, 1, Integer.MAX_VALUE);

        this.enableZookeeper = Utils.getBoolean(props, "enable.zookeeper", true);

        this.flushInterval = Utils.getIntInRange(props, "log.flush.interval", 500, 1, Integer.MAX_VALUE);
        this.flushIntervalMap = Utils.getTopicFlushIntervals(Utils.getString(props, "topic.flush.interval.ms", ""));
        this.flushSchedulerThreadRate = Utils.getInt(props, "log.default.flush.scheduler.interval.ms", 3000);
        this.defaultFlushIntervalMs = Utils.getInt(props, "log.default.flush.interval.ms", flushSchedulerThreadRate);

        this.topicPartitionMap = Utils.getTopicPartitions(Utils.getString(props, "topic.partition.count.map", ""));

        this.maxTopicNameLength = Utils.getIntInRange(props, "max.topic.name.langth", 255, 1, Integer.MAX_VALUE);
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public int getSocketSendBuffer() {
        return socketSendBuffer;
    }

    public int getSocketReceiveBuffer() {
        return socketReceiveBuffer;
    }

    public int getMaxSocketRequestSize() {
        return maxSocketRequestSize;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public int getMonitoringPeriodSecs() {
        return monitoringPeriodSecs;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public String getLogDir() {
        return logDir;
    }

    public int getLogFileSize() {
        return logFileSize;
    }

    public Map<String, Integer> getLogFileSizeMap() {
        return logFileSizeMap;
    }

    public int getLogRollHours() {
        return logRollHours;
    }

    public Map<String, Integer> getLogRollHoursMap() {
        return logRollHoursMap;
    }

    public int getLogRetentionHours() {
        return logRetentionHours;
    }

    public Map<String, Integer> getLogRetentionHoursMap() {
        return logRetentionHoursMap;
    }

    public long getLogRetentionSize() {
        return logRetentionSize;
    }

    public Map<String, Integer> getLogRetentionSizeMap() {
        return logRetentionSizeMap;
    }

    public int getLogCleanupIntervalMins() {
        return logCleanupIntervalMins;
    }

    public boolean isEnableZookeeper() {
        return enableZookeeper;
    }

    public int getFlushInterval() {
        return flushInterval;
    }

    public Map<String, Integer> getFlushIntervalMap() {
        return flushIntervalMap;
    }

    public int getFlushSchedulerThreadRate() {
        return flushSchedulerThreadRate;
    }

    public int getDefaultFlushIntervalMs() {
        return defaultFlushIntervalMs;
    }

    public Map<String, Integer> getTopicPartitionMap() {
        return topicPartitionMap;
    }

    public int getMaxTopicNameLength() {
        return maxTopicNameLength;
    }
}
