package com.jay.mykafka.cluster;

/**
 * jie.zhou
 * 2018/10/25 16:09
 */
public class Broker {
    private int id;
    private String creatorId;
    private String host;
    private int port;

    public Broker(int id, String creatorId, String host, int port) {
        this.id = id;
        this.creatorId = creatorId;
        this.host = host;
        this.port = port;
    }

    public static Broker create(int id, String brokerInfoStr) {
        String[] brokerInfo = brokerInfoStr.split(":");
        return new Broker(id, brokerInfo[0], brokerInfo[1], Integer.parseInt(brokerInfo[2]));
    }

    public String getZKString() {
        return creatorId + ":" + host + ":" + port;
    }

    public int getId() {
        return id;
    }

    public String getCreatorId() {
        return creatorId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return  "id:" + id + ",creatorId:" + creatorId + ",host:" + host + ",port:" + port;
    }
}
