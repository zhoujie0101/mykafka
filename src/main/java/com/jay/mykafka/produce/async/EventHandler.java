package com.jay.mykafka.produce.async;

import java.util.Properties;

/**
 * jie.zhou
 * 2018/10/26 16:02
 */
public interface EventHandler<T> {
    void init(Properties props);

    void close();
}
