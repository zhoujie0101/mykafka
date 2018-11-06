package com.jay.mykafka.produce.async;

import com.jay.mykafka.produce.SyncProducer;
import com.jay.mykafka.serializer.Encoder;

import java.util.List;
import java.util.Properties;

/**
 * Handler that dispatches the batched data from the queue of the asynchronous producer.
 *
 * jie.zhou
 * 2018/10/26 16:02
 */
public interface EventHandler<T> {
    /**
     * Initializes the event handler using a Properties object
     * @param props the properties used to initialize the event handler
     */
    void init(Properties props);

    /**
     * Callback to dispatch the batched data and send it to a Kafka server
     * @param events the data sent to the producer
     * @param producer the low-level producer used to send the data
     * @param serializer the serializer class
     */
    void handle(List<QueueItem<T>> events, SyncProducer producer, Encoder<T> serializer);

    /**
     * Cleans up and shuts down the event handler
     */
    void close();
}
