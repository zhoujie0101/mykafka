package com.jay.mykafka.produce;

import com.jay.mykafka.util.Utils;

import java.util.Properties;

/**
 * jie.zhou
 * 2018/10/26 16:09
 */
public class SyncProducerConfig extends SyncProducerConfigShared {
    /** the broker to which the producer sends events */
    private String host;
    /** the port on which the broker is running */
    private int port;

    public SyncProducerConfig(Properties props) {
        super(props);

        this.host = Utils.getString(props, "host");
        this.port = Utils.getInt(props, "port");
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}