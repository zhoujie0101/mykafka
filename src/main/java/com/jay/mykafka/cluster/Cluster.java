package com.jay.mykafka.cluster;

import java.util.HashMap;
import java.util.Map;

/**
 * jie.zhou
 * 2018/11/6 11:36
 */
public class Cluster {
    private Map<Integer, Broker> brokers = new HashMap<>();

    public Cluster(Iterable<Broker> it) {
        for (Broker b : it) {
            brokers.put(b.getId(), b);
        }
    }

    public Cluster() {

    }

    public Broker getBroker(int id) {
        return brokers.get(id);
    }

    public void addBroker(Broker b) {
        brokers.put(b.getId(), b);
    }

    public void removeBroker(int id) {
        brokers.remove(id);
    }

    public int size() {
        return brokers.size();
    }
}
