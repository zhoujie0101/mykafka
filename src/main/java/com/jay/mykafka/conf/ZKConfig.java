package com.jay.mykafka.conf;

import com.jay.mykafka.util.Utils;

import java.util.Properties;

/**
 * jie.zhou
 * 2018/10/25 09:30
 */
public class ZKConfig {
    protected final Properties props;

    /**
     * zk地址
     */
    protected String zkConnect;
    /**
     * zk会话超时时间，毫秒
     */
    protected int zkSessionTimeoutMs;
    /**
     * zk连接超时时间，毫秒
     */
    protected int zkConnectionTimeoutMs;
    /**
     * zk follower落后leader最大时间，毫秒
     */
    protected int zkSyncTimeMs;


    public ZKConfig(Properties props) {
        this.props = props;
        init();
    }

    protected void init() {
        this.zkConnect = Utils.getString(props, "zk.connect");
        this.zkSessionTimeoutMs = Utils.getInt(props, "zk.sessiontimeout.ms", 6000);
        this.zkConnectionTimeoutMs = Utils.getInt(props, "zk.connectiontimeout.ms", zkSessionTimeoutMs);
        this.zkSyncTimeMs = Utils.getInt(props, "zk.synctime.ms", 2000);
    }

    public String getZkConnect() {
        return zkConnect;
    }

    public int getZkSessionTimeoutMs() {
        return zkSessionTimeoutMs;
    }

    public int getZkConnectionTimeoutMs() {
        return zkConnectionTimeoutMs;
    }

    public int getZkSyncTimeMs() {
        return zkSyncTimeMs;
    }

    public Properties getProps() {
        return props;
    }
}
