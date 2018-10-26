package com.jay.mykafka.produce;

import com.jay.mykafka.util.Utils;

import java.util.Properties;

/**
 * jie.zhou
 * 2018/10/26 16:32
 */
public class SyncProducerConfigShared {
    protected Properties props;

    SyncProducerConfigShared(Properties props) {
        this.props = props;

        this.bufferSize = Utils.getInt(props, "buffer.size", 100 * 1024);
        this.connectTimeoutMs = Utils.getInt(props, "connect.timeout.ms", 5000);
        this.socketTimeoutMs = Utils.getInt(props, "socket.timeout.ms", 30000);
        this.reconnectInterval = Utils.getInt(props, "reconnect.interval", 30000);
        this.reconnectTimeInterval = Utils.getInt(props, "reconnect.time.interval.ms", 1000 * 1000 * 10);
        this.maxMessageSize = Utils.getInt(props, "max.message.size", 1000 * 1000);
    }

    private int bufferSize;
    private int connectTimeoutMs;
    private int socketTimeoutMs;
    private int reconnectInterval;
    /**
     * negative reconnect time interval means disabling this time-based reconnect feature
     */
    private int reconnectTimeInterval;
    private int maxMessageSize;

    public int getBufferSize() {
        return bufferSize;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    public int getReconnectInterval() {
        return reconnectInterval;
    }

    public int getReconnectTimeInterval() {
        return reconnectTimeInterval;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }
}

