package com.jay.mykafka.produce.async;

import com.jay.mykafka.produce.SyncProducerConfig;

import java.util.Properties;

/**
 * jie.zhou
 * 2018/10/26 10:24
 */
public class AsyncProducerConfig extends AsyncProducerConfigShared {
    private SyncProducerConfig syncProducerConfig;

    public AsyncProducerConfig(Properties props) {
        super(props);
        syncProducerConfig = new SyncProducerConfig(props);
    }

    public SyncProducerConfig getSyncProducerConfig() {
        return syncProducerConfig;
    }
}