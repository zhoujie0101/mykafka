package com.jay.mykafka.produce;

import com.jay.mykafka.api.ProducerRequest;
import com.jay.mykafka.cluster.Broker;
import com.jay.mykafka.cluster.Partition;
import com.jay.mykafka.common.UnavailableProducerException;
import com.jay.mykafka.message.ByteBufferMessageSet;
import com.jay.mykafka.produce.async.*;
import com.jay.mykafka.serializer.Encoder;
import com.jay.mykafka.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * jie.zhou
 * 2018/10/26 15:03
 */
public class ProducerPool<V> {

    public static final Logger LOGGER = LoggerFactory.getLogger(ProducerPool.class);

    private ProducerConfig config;
    private Encoder<V> serializer;
    private EventHandler<V> eventHandler;
    private CallbackHandler<V> ckHandler;
    //brokerId -> SyncProducer
    private ConcurrentMap<Integer, SyncProducer> syncProducers;
    //brokerId -> AsyncProducer
    private ConcurrentMap<Integer, AsyncProducer<V>> asyncProducers;
    private boolean sync;

    public ProducerPool(ProducerConfig config, Encoder<V> serializer) {
        this(config, serializer, Utils.getObject(config.getAsyncProducerConfigShared().getEventHandler()),
                Utils.getObject(config.getAsyncProducerConfigShared().getCkHanler()));
    }

    public ProducerPool(ProducerConfig config, Encoder<V> serializer, EventHandler<V> eventHandler,
                            CallbackHandler<V> ckHandler) {
        this(config, serializer, eventHandler, ckHandler, new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
    }

    public ProducerPool(ProducerConfig config, Encoder<V> serializer, EventHandler<V> eventHandler,
                        CallbackHandler<V> ckHandler, ConcurrentMap<Integer, SyncProducer> syncProducers,
                        ConcurrentMap<Integer, AsyncProducer<V>> asyncProducers) {
        if (serializer == null) {
            throw new IllegalArgumentException("serializer passed in is null!");
        }
        if (eventHandler == null) {
            eventHandler = new DefaultEventHandler<>(new AsyncProducerConfig(config.getProps(),
                    config.getSyncProducerConfig()), ckHandler);
        }
        this.config = config;
        this.serializer = serializer;
        this.eventHandler = eventHandler;
        this.ckHandler = ckHandler;
        this.syncProducers = syncProducers;
        this.asyncProducers = asyncProducers;
        if ("sync".equals(config.getProduceType())) {
            sync = true;
        } else if ("async".equals(config.getProduceType())) {
            sync = false;
        } else {
            throw new IllegalArgumentException("Valid values for producer.type are sync/async");
        }
    }

    /**
     * selects either a synchronous or an asynchronous producer, for
     * the specified broker id and calls the send API on the selected
     * producer to publish the data to the specified broker partition
     * @param poolData the producer pool request object
     */
    public void send(List<ProducePoolData<V>> poolData) {
        Set<Integer> distinctBrokerIds = poolData.stream().map(req -> req.getPartition().getBrokerId())
                .collect(Collectors.toSet());
        //brokerId --> list
        Map<Integer, List<ProducePoolData<V>>> dataMap = new HashMap<>(poolData.size());
        for (ProducePoolData<V> data : poolData) {
            dataMap.computeIfAbsent(data.getPartition().getBrokerId(), k -> new ArrayList<>())
                    .add(data);
        }
        distinctBrokerIds.forEach(brokerId -> {
            List<ProducePoolData<V>> requestForCurrentBroker = dataMap.get(brokerId);
            if (sync) {
                List<ProducerRequest> requests = requestForCurrentBroker.stream().map(req -> {
                    ByteBufferMessageSet message = new ByteBufferMessageSet(
                            req.getData().stream().map(d -> serializer.toMessage(d)).collect(Collectors.toList())
                    );
                    return new ProducerRequest(req.getTopic(), req.getPartition().getPartitionId(),
                            message);
                }).collect(Collectors.toList());
                SyncProducer producer = syncProducers.get(brokerId);
                if (producer == null) {
                    throw new UnavailableProducerException("Producer pool has not been initialized correctly. " +
                            "Sync Producer for broker " + brokerId + " does not exist in the pool");
                }
                producer.send(requests);
            } else {
                AsyncProducer<V> producer = asyncProducers.get(brokerId);
                if (producer == null) {
                    throw new UnavailableProducerException("Producer pool has not been initialized correctly. " +
                            "Async Producer for broker " + brokerId + " does not exist in the pool");
                }
                requestForCurrentBroker.forEach(req ->
                        req.getData().forEach(data ->
                                producer.send(req.getTopic(), req.getPartition().getPartitionId(), data)
                        )
                );
            }
        });
    }

    public void addProducer(Broker broker) {
        Properties props = new Properties();
        props.put("host", broker.getHost());
        props.put("port", broker.getPort());
        props.putAll(config.getProps());
        SyncProducer syncProducer = new SyncProducer(new SyncProducerConfig(props));
        if (sync) {
            syncProducers.put(broker.getId(), syncProducer);
        } else {
            AsyncProducer<V> asyncProducer = new AsyncProducer<>(new AsyncProducerConfig(props), syncProducer,
                    serializer, eventHandler, config.getAsyncProducerConfigShared().getEventHandlerProps(),
                    ckHandler, config.getAsyncProducerConfigShared().getCkHandlerProps());
            asyncProducer.start();
            asyncProducers.put(broker.getId(), asyncProducer);
        }
    }

    public <T> ProducePoolData<T> createProducePoolData(String topic, Partition partition, List<T> data) {
        return new ProducePoolData<>(topic, partition, data);
    }

    class ProducePoolData<T> {
        private String topic;
        private Partition partition;
        private List<T> data;

        public ProducePoolData(String topic, Partition partition, List<T> data) {
            this.topic = topic;
            this.partition = partition;
            this.data = data;
        }

        public String getTopic() {
            return topic;
        }

        public Partition getPartition() {
            return partition;
        }

        public List<T> getData() {
            return data;
        }
    }
}
