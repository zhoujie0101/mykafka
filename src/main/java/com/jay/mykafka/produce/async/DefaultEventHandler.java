package com.jay.mykafka.produce.async;

import com.jay.mykafka.api.ProducerRequest;
import com.jay.mykafka.cluster.TopicPartition;
import com.jay.mykafka.message.ByteBufferMessageSet;
import com.jay.mykafka.message.Message;
import com.jay.mykafka.produce.SyncProducer;
import com.jay.mykafka.serializer.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * jie.zhou
 * 2018/10/26 16:51
 */
public class DefaultEventHandler<T> implements EventHandler<T> {
    public static final Logger LOGGER = LoggerFactory.getLogger(DefaultEventHandler.class);

    private AsyncProducerConfig config;
    private CallbackHandler<T> cbkHandler;

    public DefaultEventHandler(AsyncProducerConfig config, CallbackHandler<T> ckHandler) {
        this.config = config;
        this.cbkHandler = ckHandler;
    }

    @Override
    public void init(Properties props) {

    }

    @Override
    public void handle(List<QueueItem<T>> events, SyncProducer syncProducer, Encoder<T> serializer) {
        if (events == null || events.size() == 0) {
            return;
        }
        List<QueueItem<T>> processedEvents = cbkHandler.beforeSendingData(events);

        send(serialize(collate(processedEvents), serializer), syncProducer);
    }

    private void send(Map<TopicPartition, ByteBufferMessageSet> messagePerTopicPartition, SyncProducer syncProducer) {
        List<ProducerRequest> requests = new ArrayList<>(messagePerTopicPartition.size());
        messagePerTopicPartition.forEach((tp, msg) -> {
            ProducerRequest req = new ProducerRequest(tp.getTopic(), tp.getPartition(), msg);
            requests.add(req);
        });
        //TODO add retry
        syncProducer.send(requests);
    }

    private Map<TopicPartition, List<T>> collate(List<QueueItem<T>> events) {
        Map<TopicPartition, List<T>> collatedEvents = new HashMap<>();
        events.forEach(event -> {
            TopicPartition tp = new TopicPartition(event.getTopic(), event.getPartition());
            collatedEvents.computeIfAbsent(tp, k -> new ArrayList<>())
                    .add(event.getEvent());
        });

        return collatedEvents;
    }

    private Map<TopicPartition, ByteBufferMessageSet> serialize(Map<TopicPartition, List<T>> eventsPerTopicPartition,
                                                                Encoder<T> serializer) {
        Map<TopicPartition, ByteBufferMessageSet> messagesPerTopicPartition = new HashMap<>(
                eventsPerTopicPartition.size());
        eventsPerTopicPartition.forEach((tp, events) -> {
            List<Message> messages = events.stream()
                    .map(serializer::toMessage)
                    .collect(Collectors.toList());
            messagesPerTopicPartition.put(tp, new ByteBufferMessageSet(messages));
        });

        return messagesPerTopicPartition;
    }

    @Override
    public void close() {

    }
}
