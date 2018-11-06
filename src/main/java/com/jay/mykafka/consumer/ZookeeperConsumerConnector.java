package com.jay.mykafka.consumer;

import com.jay.mykafka.cluster.Cluster;
import com.jay.mykafka.cluster.Partition;
import com.jay.mykafka.serializer.Decoder;
import com.jay.mykafka.serializer.DefaultDecoder;
import com.jay.mykafka.util.ZKStringSerializer;
import com.jay.mykafka.util.ZKUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class handles the consumers interaction with zookeeper
 *
 * Directories:
 * 1. Consumer id registry:
 * /consumers/[group_id]/ids[consumer_id] -> topic1,...topicN
 * A consumer has a unique consumer id within a consumer group. A consumer registers its id as an ephemeral znode
 * and puts all topics that it subscribes to as the value of the znode. The znode is deleted when the client is gone.
 * A consumer subscribes to event changes of the consumer id registry within its group.
 *
 * The consumer id is picked up from configuration, instead of the sequential id assigned by ZK. Generated sequential
 * ids are hard to recover during temporary connection loss to ZK, since it's difficult for the client to figure out
 * whether the creation of a sequential znode has succeeded or not. More details can be found at
 * (http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)
 *
 * 2. Broker node registry:
 * /brokers/[0...N] --> { "host" : "host:port",
 *                        "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
 *                                    "topicN": ["partition1" ... "partitionN"] } }
 * This is a list of all present broker brokers. A unique logical node id is configured on each broker node. A broker
 * node registers itself on start-up and creates a znode with the logical node id under /brokers. The value of the znode
 * is a JSON String that contains (1) the host name and the port the broker is listening to, (2) a list of topics that
 * the broker serves, (3) a list of logical partitions assigned to each topic on the broker.
 * A consumer subscribes to event changes of the broker node registry.
 *
 * 3. Partition owner registry:
 * /consumers/[group_id]/owner/[topic]/[broker_id-partition_id] --> consumer_node_id
 * This stores the mapping before broker partitions and consumers. Each partition is owned by a unique consumer
 * within a consumer group. The mapping is reestablished after each rebalancing.
 *
 * 4. Consumer offset tracking:
 * /consumers/[group_id]/offsets/[topic]/[broker_id-partition_id] --> offset_counter_value
 * Each consumer tracks the offset of the latest message consumed for each partition.
 *
 * jie.zhou
 * 2018/11/5 18:52
 */
public class ZookeeperConsumerConnector implements ConsumerConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperConsumerConnector.class);

    private ConsumerConfig config;
    private boolean enableFetcher;
    private AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private Object rebalanceLock = new Object();
    private ZkClient zkClient;
    private String consumerIdString;
    private Fetcher fetcher;
    private ScheduledExecutorService autoCommitScheduler;
    private ZKGroupDir dir;
    private ZKSessionExpireListener sessionExpireListener;
    private ZKRebalancerListener rebalancerListener;
    //topic -> (consumerThreadId -> queue)
    private ConcurrentMap<String, ConcurrentMap<String, BlockingQueue<FetchedDataChunk>>> topicThreadIdAndQueues
            = new ConcurrentHashMap<>();

    //topic -> (partition -> partInfo)
    private ConcurrentMap<String, ConcurrentMap<Partition, PartitionTopicInfo>> topicRegistry
            = new ConcurrentHashMap<>();

    public ZookeeperConsumerConnector(ConsumerConfig config) {
        this(config, true);
    }

    public ZookeeperConsumerConnector(ConsumerConfig config, boolean enableFetcher) {
        this.config = config;
        this.enableFetcher = enableFetcher;

        String consumerUuid = null;
        if (config.getConsumerId() != null) {
            consumerUuid = config.getConsumerId();
        } else {
            UUID uuid = UUID.randomUUID();
            try {
                consumerUuid = String.format("%s-%d-%s", InetAddress.getLocalHost().getHostName(),
                        System.currentTimeMillis(), Long.toHexString(uuid.getMostSignificantBits()).substring(0, 8));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        this.consumerIdString = config.getGroupId() + "-" + consumerUuid;

        dir = new ZKGroupDir(config.getGroupId());

        connectZK();  // init zkclient

        createFetcher();  // init fetcher

        scheduleAutoCommit();  // schedule autocommit task
    }

    private void connectZK() {
        zkClient = new ZkClient(config.getZkConnect(), config.getZkSessionTimeoutMs(),
                config.getZkConnectionTimeoutMs(), new ZKStringSerializer());
    }

    private void createFetcher() {
        if (enableFetcher) {
            fetcher = new Fetcher(config, zkClient);
        }
    }

    private void scheduleAutoCommit() {
        if (config.isAutoCommit()) {
            autoCommitScheduler = new ScheduledThreadPoolExecutor(1, r -> {
                Thread t = new Thread(r, "kafka-consumer-autocommit-1");
                t.setDaemon(false);
                return t;
            });
            autoCommitScheduler.scheduleAtFixedRate(this::autoCommit, config.getAutoCommitIntervalMs(),
                    config.getAutoCommitIntervalMs(), TimeUnit.MILLISECONDS);
        }
    }

    private void autoCommit() {
        try {
            commitOffsets();
        } catch (Exception e) {
            LOGGER.error("exception during autoCommit", e);
        }
    }

    @Override
    public <T> Map<String, List<KafkaStream<T>>> createMessageStreams(Map<String, Integer> topicCountMap) {
        return createMessageStreams(topicCountMap, (Decoder<T>) new DefaultDecoder());
    }

    @Override
    public <T> Map<String, List<KafkaStream<T>>> createMessageStreams(Map<String, Integer> topicCountMap,
                                                                      Decoder<T> decoder) {
        if (topicCountMap == null) {
            throw new RuntimeException("topicCountMap is null");
        }
        if (decoder == null) {
            decoder = (Decoder<T>) new DefaultDecoder();
        }

        return consume(topicCountMap, decoder);
    }

    private <T> Map<String, List<KafkaStream<T>>> consume(Map<String, Integer> topicCountMap, Decoder<T> decoder) {
        TopicCount tc = new StaticTopicCount(consumerIdString, topicCountMap);

        registerConsumerInZK(tc);

        Map<String, Set<String>> consumerThreadIdsPerTopic = tc.getConsumerThreadIsPerTopic();
        consumerThreadIdsPerTopic.forEach((topic, consumerThreadIds) -> {
            //threadId --> queue
            ConcurrentMap<String, BlockingQueue<FetchedDataChunk>> threadIdAndQueues
                    = new ConcurrentHashMap<>();
            topicThreadIdAndQueues.put(topic, threadIdAndQueues);

            consumerThreadIds.forEach(threadId -> {
                BlockingQueue<FetchedDataChunk> queue = new LinkedBlockingQueue<>(config.getMaxQueuedChunks());
                threadIdAndQueues.put(threadId, queue);
            });
        });

        reinitializeConsumer(tc);

        return null;
    }

    private void registerConsumerInZK(TopicCount tc) {
        ZKUtils.createEphemeralPathExpectConflict(zkClient, dir.getConsumerRegistryDir() + "/" + consumerIdString,
                tc.dbString());
    }

    private void reinitializeConsumer(TopicCount tc) {
        if (rebalancerListener == null) {
            Map<String, List<KafkaStream<?>>> streamsMap = new HashMap<>();
            topicThreadIdAndQueues.keySet().forEach(topic ->
                streamsMap.computeIfAbsent(topic, k -> new ArrayList<>())
                        .add(new KafkaStream<>())
            );
            rebalancerListener = new ZKRebalancerListener(streamsMap);
            zkClient.subscribeChildChanges(dir.getConsumerRegistryDir(), rebalancerListener);
        }

        if (sessionExpireListener == null) {
            sessionExpireListener = new ZKSessionExpireListener(tc, rebalancerListener);
            zkClient.subscribeStateChanges(sessionExpireListener);
        }

        rebalancerListener.syncedRebalance();
    }

    @Override
    public void commitOffsets() {

    }

    @Override
    public void shutdown() {

    }

    class ZKSessionExpireListener implements IZkStateListener {
        private TopicCount tc;
        private ZKRebalancerListener rebalancerListener;

        public ZKSessionExpireListener(TopicCount tc, ZKRebalancerListener rebalancerListener) {
            this.tc = tc;
            this.rebalancerListener = rebalancerListener;
        }

        @Override
        public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {

        }

        /**
         *  When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
         *  connection for us. We need to release the ownership of the current consumer and re-register this
         *  consumer in the consumer registry and trigger a rebalance.
         */
        @Override
        public void handleNewSession() throws Exception {
            LOGGER.info("ZK expired; release old broker parition ownership; re-register consumer " + consumerIdString);
            registerConsumerInZK(tc);
            rebalancerListener.syncedRebalance();
        }

        @Override
        public void handleSessionEstablishmentError(Throwable error) throws Exception {

        }
    }

    class ZKRebalancerListener implements IZkChildListener {
        //topic -> streams
        private Map<String, List<KafkaStream<?>>> streams;

        public ZKRebalancerListener(Map<String, List<KafkaStream<?>>> streams) {
            this.streams = streams;
        }

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {

        }

        public void syncedRebalance() {
            synchronized (rebalanceLock) {
                for (int i = 0; i < config.getMaxRebalanceRetries(); i++) {
                    boolean done = false;
                    Cluster cluster = ZKUtils.getCluster(zkClient);
                    try {
                        done = rebalance(cluster);
                    } catch (Exception e) {
                        /** occasionally, we may hit a ZK exception because the ZK state is changing while we are iterating.
                         * For example, a ZK node can disappear between the time we get all children and the time we try to get
                         * the value of a child. Just let this go since another rebalance will be triggered.
                         **/
                        LOGGER.info("exception during rebalance", e);
                    }
                    if (done) {
                        return;
                    }
                    //TODO do sth when not done
                }
            }
        }

        private boolean rebalance(Cluster cluster) {
            TopicCount tc = TopicCount.constructStatic(config.getGroupId(), consumerIdString, zkClient);
            Map<String, Set<String>> myTopicThreadIds = tc.getConsumerThreadIsPerTopic();
            //topic -> consumerThreadIds
            Map<String, List<String>> consumersPerTopic = ZKUtils.getConsumersPerTopic(zkClient, dir);
            //topic -> partitionIds
            Map<String, List<String>> partitionsPerTopic = ZKUtils.getPartitionsPerTopic(zkClient,
                    consumersPerTopic.keySet());
            //TODO close fetcher

            releasePartitionOwnership();

            ConcurrentMap<String, ConcurrentMap<Partition, PartitionTopicInfo>> currentTopicRegistry
                    = new ConcurrentHashMap<>();
            //topic -> (partition -> threadId)
            Map<String, Map<String, String>> partitionOwnershipDecision = new HashMap<>();

            myTopicThreadIds.forEach((topic, consumerThreadIds) -> {
                currentTopicRegistry.put(topic, new ConcurrentHashMap<>());

                ZKGroupTopicDir topicDir = new ZKGroupTopicDir(config.getGroupId(), topic);
                List<String> curConsumers = consumersPerTopic.get(topic);
                List<String> curPartitions = partitionsPerTopic.get(topic);

                int nPartsPerConsumer = curPartitions.size() / curConsumers.size();
                int nConsumersWithExtraPart = curPartitions.size() % curConsumers.size();

                consumerThreadIds.forEach(consumerThreadId -> {
                    int myConsumerPosition = curConsumers.indexOf(consumerThreadId);
                    int startPart = myConsumerPosition * nPartsPerConsumer
                            + Math.min(myConsumerPosition, nConsumersWithExtraPart);
                    int nParts = nPartsPerConsumer;
                    if (myConsumerPosition + 1 <= nConsumersWithExtraPart) {
                        nParts += 1;
                    }
                    for (int i = startPart; i < startPart + nParts; i++) {
                        String partition = curPartitions.get(startPart);
                        addPartitionInfo(currentTopicRegistry, topicDir, partition, topic,
                                consumerThreadId);
                        partitionOwnershipDecision.computeIfAbsent(topic, k -> new HashMap<>())
                                                .put(partition, consumerThreadId);
                    }
                });
            });

            /**
             * move the partition ownership here, since that can be used to indicate a truly successful rebalancing attempt
             * A rebalancing attempt is completed successfully only after the fetchers have been started correctly
             */
            if(reflectPartitionOwnershipDecision(partitionOwnershipDecision)) {
                topicRegistry = currentTopicRegistry;
                updateFetcher(cluster);
            }

            return false;
        }

        private void addPartitionInfo(ConcurrentMap<String, ConcurrentMap<Partition, PartitionTopicInfo>> currentTopicRegistry,
                                      ZKGroupTopicDir topicDir, String partitionString, String topic, String consumerThreadId) {
            Partition partition = Partition.parse(partitionString);
            Map<Partition, PartitionTopicInfo> partInfoMap = currentTopicRegistry.get(topic);
            String offsetPath = topicDir.getConsumerOffsetDir() + "/" + partition.name();
            String offsetString = zkClient.readData(offsetPath);
            long offset;
            if (offsetString == null) {
                //TODO fetch offset
                offset = 0L;
            } else {
                offset = Long.parseLong(offsetString);
            }
            BlockingQueue<FetchedDataChunk> queue = topicThreadIdAndQueues.get(topic).get(consumerThreadId);
            AtomicLong consumeOffset = new AtomicLong(offset);
            AtomicLong fetchOffset = new AtomicLong(offset);
            PartitionTopicInfo partInfo = new PartitionTopicInfo(topic,
                                                                partition.getBrokerId(),
                                                                partition.getPartitionId(),
                                                                queue,
                                                                consumeOffset,
                                                                fetchOffset,
                                                                new AtomicInteger(config.getFetchSize()));
            partInfoMap.put(partition, partInfo);
        }

        private boolean reflectPartitionOwnershipDecision(Map<String, Map<String, String>> partitionOwnershipDecision) {
            Map<String, List<String>> successfullyOwnedPartitions = new HashMap<>();
            Map<String, List<String>> failedOwnedPartitions = new HashMap<>();
            partitionOwnershipDecision.forEach((topic, partitionMap) -> {
                List<String> successfullyPartitions = new ArrayList<>();
                successfullyOwnedPartitions.put(topic, successfullyPartitions);
                    partitionMap.forEach((partition, consumerThreadId) -> {
                        ZKGroupTopicDir groupTopicDir = new ZKGroupTopicDir(config.getGroupId(), topic);
                        String partitionOwnerPath = groupTopicDir.getConsumerOwnerDir() + "/" + partition;
                        try {
                            ZKUtils.createEphemeralPathExpectConflict(zkClient, partitionOwnerPath, consumerThreadId);
                            successfullyPartitions.add(partition);
                        } catch (ZkNodeExistsException e1) {
                            failedOwnedPartitions.computeIfAbsent(topic, k -> new ArrayList<>())
                                                    .add(partition);
                        } catch (Exception e2) {
                            throw e2;
                        }
                    });
                }
            );

            boolean success = true;
            if (failedOwnedPartitions.size() > 0) {
                success = false;
                successfullyOwnedPartitions.forEach((topic, partitions) -> {
                    ZKGroupTopicDir groupTopicDir = new ZKGroupTopicDir(config.getGroupId(), topic);
                    partitions.forEach(partition -> {
                        zkClient.delete(groupTopicDir.getConsumerOwnerDir() + "/" + partition);
                    });
                });
            }

            return success;
        }

        private void releasePartitionOwnership() {
            topicRegistry.forEach((topic, partMap) -> {
                ZKGroupTopicDir groupTopicDir = new ZKGroupTopicDir(config.getGroupId(), topic);
                partMap.forEach((part, partInfo) -> {
                    String ownerPath = groupTopicDir.getConsumerOwnerDir() + "/" + part.toString();
                    zkClient.delete(ownerPath);
                });
            });
        }

        private void updateFetcher(Cluster cluster) {
            if (fetcher != null) {
                List<PartitionTopicInfo> allPartitionInfo = new ArrayList<>();
                topicRegistry.forEach((topic, partitionMap) ->
                    partitionMap.forEach((partition, partInfo) ->
                        allPartitionInfo.add(partInfo)
                    )
                );
                fetcher.startConnections(cluster, allPartitionInfo);
            }
        }
    }
}
