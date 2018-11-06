package com.jay.mykafka.log;

import com.jay.mykafka.api.OffsetRequest;
import com.jay.mykafka.cluster.TopicPartition;
import com.jay.mykafka.conf.KafkaConfig;
import com.jay.mykafka.server.KafkaZookeeper;
import com.jay.mykafka.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * kafka log 管理服务
 * jie.zhou
 * 2018/10/25 11:19
 */
public class LogManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogManager.class);

    private KafkaConfig config;
    /**
     * 日志目录
     */
    private File logDir;
    /**
     * 默认分区数
     */
    private int numPartitions;
    private Map<String, Integer> logFileSizeMap;
    private int flushInterval;
    private Map<String, Integer> topicPartitionsMap;
    private Object logCreationLock = new Object();
    private Random rand = new Random();
    private CountDownLatch startupLatch;
    private ScheduledThreadPoolExecutor logCleanerScheduler;
    private AtomicLong logCleanerThreadId = new AtomicLong(0);
    private ScheduledThreadPoolExecutor logFlusherScheduler;
    private AtomicLong logFlusherThreadId = new AtomicLong(0);
    private Map<String, Integer> logFlushIntervalMap;
    private Map<String, Integer> logRetentionSizeMap;
    private Map<String, Long> logRetentionMsMap;
    private Map<String, Long> logRollMsMap;
    private long logRollDefaultIntervalMs;

    private KafkaZookeeper kafkaZookeeper;

    //topic -> (partition -> log)
    private ConcurrentMap<String, ConcurrentMap<Integer, Log>> logs;

    public LogManager(KafkaConfig config) {
        this.config = config;

        logDir = new File(config.getLogDir());
        numPartitions = config.getNumPartitions();
        logFileSizeMap = config.getLogFileSizeMap();
        flushInterval = config.getFlushInterval();
        topicPartitionsMap = config.getTopicPartitionMap();
        logFlushIntervalMap = config.getFlushIntervalMap();
        logRetentionSizeMap = config.getLogRetentionSizeMap();
        logRetentionMsMap = getMsMap(config.getLogRetentionHoursMap());
        logRollMsMap = getMsMap(config.getLogRollHoursMap());
        logRollDefaultIntervalMs = 1000 * 60 * 60 * config.getLogRollHours();

        initLogs();

        startClean();

        if (config.isEnableZookeeper()) {
            startupLatch = new CountDownLatch(1);
            kafkaZookeeper = new KafkaZookeeper(config, this);
            kafkaZookeeper.startup();
        }
    }

    public void startup() {
        if (config.isEnableZookeeper()) {
            kafkaZookeeper.registerBrokerInZK();
            Iterator<String> iter = logs.keySet().iterator();
            while (iter.hasNext()) {
                kafkaZookeeper.registerTopicInZK(iter.next());
            }
        }

        logFlusherScheduler = new ScheduledThreadPoolExecutor(config.getNumThreads(), r -> {
            Thread t = new Thread(r, "kafka-logflusher-" + logFlusherThreadId.getAndIncrement());
            t.setDaemon(false);
            return t;
        });

        LOGGER.info("Starting log flusher every " + config.getFlushSchedulerThreadRate()
                + " ms with the following overrides " + logFlushIntervalMap);
        logFlusherScheduler.scheduleAtFixedRate(this::flushAllLogs, config.getFlushSchedulerThreadRate(),
                config.getFlushSchedulerThreadRate(), TimeUnit.MILLISECONDS);
    }

    public void close() {

    }

    private void initLogs() {
        logs = new ConcurrentHashMap<>();

        if (!logDir.exists()) {
            LOGGER.info("No log dir found, creating '" + logDir.getAbsolutePath() + "'");
            if (!logDir.mkdir()) {
                throw new IllegalArgumentException("Can not create log dir '" + logDir.getAbsolutePath() + "'");
            }
        }
        if (!logDir.isDirectory() || !logDir.canRead()) {
            throw new IllegalArgumentException(logDir.getAbsolutePath() + " is not a readable log dir");
        }
        File[] subDirs = logDir.listFiles();
        if (subDirs == null || subDirs.length == 0) {
            return;
        }
        for (File dir : subDirs) {
            if (!dir.isDirectory()) {
                LOGGER.warn("Skipping unexplainable file '" + dir.getAbsolutePath() + "'--should it be there?");
            } else {
                LOGGER.info("Loading log '" + dir.getName() + "'");

                TopicPartition tp = Utils.getTopicPartition(dir.getName());
                long rollIntervalMs = logRollMsMap.getOrDefault(tp.getTopic(),
                        (long) config.getDefaultFlushIntervalMs());
                long maxLogFileSize = logFileSizeMap.getOrDefault(tp.getTopic(), config.getLogFileSize());
                Log log = new Log(dir, maxLogFileSize, config.getMaxMessageSize(), flushInterval, rollIntervalMs);
                logs.computeIfAbsent(tp.getTopic(), k -> new ConcurrentHashMap<>())
                        .put(tp.getPartition(), log);
            }
        }
    }

    /**
     * schedule cleanup task to delete old logs
     */
    private void startClean() {
        logCleanerScheduler = new ScheduledThreadPoolExecutor(config.getNumThreads(), r -> {
            Thread t = new Thread(r, "kafka-logcleaner-" + logCleanerThreadId.getAndIncrement());
            t.setDaemon(false);
            return t;
        });
        logCleanerScheduler.scheduleAtFixedRate(this::cleanupLogs, 60 * 1000,
                config.getLogCleanupIntervalMins() * 60 * 1000L, TimeUnit.MILLISECONDS);
    }

    private void cleanupLogs() {

    }

    private void flushAllLogs() {

    }

    private Map<String, Long> getMsMap(Map<String, Integer> map) {
        Map<String, Long> ret = new HashMap<>(map.size());
        map.forEach((key, value) -> ret.put(key, value * 60 * 60 * 1000L));

        return ret;
    }

    public Map<String, Integer> getTopicPartitionsMap() {
        return topicPartitionsMap;
    }

    public int chooseRandomPartition(String topic) {
        return rand.nextInt(topicPartitionsMap.getOrDefault(topic, numPartitions));
    }

    public Log getOrCreateLog(String topic, int partition) {
        boolean newTopic = false;
        ConcurrentMap<Integer, Log> partitionLogMap =  logs.get(topic);
        if (partitionLogMap == null) {
            ConcurrentMap oldPartitionLogMap = logs.putIfAbsent(topic, new ConcurrentHashMap<>());
            if (oldPartitionLogMap == null) {
                newTopic = true;
            }
            partitionLogMap = logs.get(topic);
        }
        Log log = partitionLogMap.get(partition);
        if (log == null) {
            log = createLog(topic, partition);
            Log oldLog = partitionLogMap.putIfAbsent(partition, log);
            if (oldLog != null) {
                log.close();
                log = oldLog;
            } else {
                LOGGER.info("Created log for '" + topic + "'-" + partition);
            }
        }

        if (newTopic) {
            kafkaZookeeper.registerTopicInZK(topic);
        }

        return log;
    }

    private Log createLog(String topic, int partition) {
        synchronized (logCreationLock) {
            File dir = new File(logDir, topic + "-" + partition);
            dir.mkdirs();
            long rollIntervalMs = logRollMsMap.getOrDefault(topic, logRollDefaultIntervalMs);
            int maxLogFileSize = logFileSizeMap.getOrDefault(topic, config.getLogFileSize());
            return new Log(dir, maxLogFileSize, config.getMaxMessageSize(), flushInterval, rollIntervalMs);
        }
    }

    public long[] getOffsets(OffsetRequest request) {
        Log log = getLog(request.getTopic(), request.getPartition());
        if (log != null) {
            return log.getOffsetsBefore(request);
        } else {
            return Log.getEmptyOffsets(request);
        }
    }

    public Log getLog(String topic, int partition) {
        Map<Integer, Log> partitionMap = logs.get(topic);
        if (partitionMap == null) {
            return null;
        }
        return partitionMap.get(partition);
    }
}
