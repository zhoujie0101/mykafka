package com.jay.mykafka.produce.async;

import java.util.Properties;

/**
 * jie.zhou
 * 2018/10/26 16:51
 */
public class DefaultEventHandler<T> implements EventHandler<T> {

    public DefaultEventHandler(AsyncProducerConfig config, CallbackHandler ckHandler) {

    }

    @Override
    public void init(Properties props) {

    }
}
