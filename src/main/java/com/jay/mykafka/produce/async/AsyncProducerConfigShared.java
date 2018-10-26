package com.jay.mykafka.produce.async;

import com.jay.mykafka.util.Utils;

import java.util.Properties;

/**
 * jie.zhou
 * 2018/10/26 16:31
 */
public class AsyncProducerConfigShared {
    private Properties props;
    /** maximum time, in milliseconds, for buffering data on the producer queue */
    private int queueTime;
    /** the maximum size of the blocking queue for buffering on the producer */
    private int queueSize;
    /**
     * Timeout for event enqueue:
     * 0: events will be enqueued immediately or dropped if the queue is full
     * -ve: enqueue will block indefinitely if the queue is full
     * +ve: enqueue will block up to this many milliseconds if the queue is full
     */
    private int enqueueTimeoutMs;
    /** the number of messages batched at the producer */
    private int batchSize;
    /** the serializer class for events */
    private String serializerClass;
    /** the callback handler for one or multiple events */
    private String ckHanler;
    /** properties required to initialize the callback handler */
    private Properties ckHandlerProps;
    /** the handler for events */
    private String eventHandler;
    /** properties required to initialize the callback handler */
    private Properties eventHandlerProps;

    public AsyncProducerConfigShared(Properties props) {
        this.props = props;

        this.queueTime = Utils.getInt(props, "queue.time", 5000);
        this.queueSize = Utils.getInt(props, "queue.size", 10000);
        this.enqueueTimeoutMs = Utils.getInt(props, "queue.enqueueTimeout.ms", 0);
        this.batchSize = Utils.getInt(props, "batch.size", 200);
        this.serializerClass = Utils.getString(props, "serializer.class", "com.jay.mykafka.serializer.DefaultEncoder");
        this.ckHanler = Utils.getString(props, "callback.handler", null);
        this.ckHandlerProps = Utils.getProps(props, "callback.handler.props", null);
        this.eventHandler = Utils.getString(props, "event.handler", null);
        this.eventHandlerProps = Utils.getProps(props, "event.handler.props", null);
    }

    public int getQueueTime() {
        return queueTime;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public int getEnqueueTimeoutMs() {
        return enqueueTimeoutMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public String getSerializerClass() {
        return serializerClass;
    }

    public String getCkHanler() {
        return ckHanler;
    }

    public Properties getCkHandlerProps() {
        return ckHandlerProps;
    }

    public String getEventHandler() {
        return eventHandler;
    }

    public Properties getEventHandlerProps() {
        return eventHandlerProps;
    }
}

