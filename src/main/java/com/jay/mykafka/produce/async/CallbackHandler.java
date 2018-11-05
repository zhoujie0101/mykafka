package com.jay.mykafka.produce.async;

import java.util.Properties;

/**
 * jie.zhou
 * 2018/10/26 16:03
 */
public interface CallbackHandler<T> {
    void init(Properties props);

    void beforeEnqueue(QueueItem<T> item);

    void afterEnqueue(QueueItem<T> item);

    void close();
}
