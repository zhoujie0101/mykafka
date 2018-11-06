package com.jay.mykafka.produce.async;

import java.util.List;
import java.util.Properties;

/**
 * Callback handler APIs for use in the async producer. The purpose is to
 * give the user some callback handles to insert custom functionality at
 * various stages as the data flows through the pipeline of the async producer
 *
 * jie.zhou
 * 2018/10/26 16:03
 */
public interface CallbackHandler<T> {
    /**
     * Initializes the callback handler using a Properties object
     * @param props properties used to initialize the callback handler
     */
    void init(Properties props);

    /**
     * Callback to process the data before it enters the batching queue
     * of the asynchronous producer
     * @param data the data sent to the producer
     * @return the processed data that enters the queue
     */
    void beforeEnqueue(QueueItem<T> data);

    /**
     * Callback to process the data right after it enters the batching queue
     * of the asynchronous producer
     * @param data the data sent to the producer
     * @param added flag that indicates if the data was successfully added to the queue
     */
    void afterEnqueue(QueueItem<T> data, boolean added);

    /**
     * Callback to process the batched data right before it is being sent by the
     * handle API of the event handler
     * @param data the batched data received by the event handler
     * @return the processed batched data that gets sent by the handle() API of the event handler
     */
    List<QueueItem<T>> beforeSendingData(List<QueueItem<T>> data);

    /**
     * Cleans up and shuts down the callback handler
     */
    void close();
}
