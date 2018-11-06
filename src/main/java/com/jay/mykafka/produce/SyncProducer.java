package com.jay.mykafka.produce;

import com.jay.mykafka.api.MultiProducerRequest;
import com.jay.mykafka.api.ProducerRequest;
import com.jay.mykafka.cluster.Broker;
import com.jay.mykafka.common.UnableToConnectBrokerException;
import com.jay.mykafka.network.BoundedByteBufferSend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Channel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * jie.zhou
 * 2018/10/26 16:04
 */
public class SyncProducer {
    public static final Logger LOGGER = LoggerFactory.getLogger(ProducerPool.class);

    public static final int MAX_CONNECT_BACKOFF_MS = 60000;
    private SocketChannel channel;
    private int sentOnChannel;
    private volatile boolean shutdown = false;
    private Object lock = new Object();
//    Random rand = new Random();
    private long lastConnectTime;

    private SyncProducerConfig config;

    public SyncProducer(SyncProducerConfig config) {
        this.config = config;
//        lastConnectTime = (long) (System.currentTimeMillis() - rand.nextDouble() * config.getReconnectInterval());
        lastConnectTime = System.currentTimeMillis();
    }

    public void send(List<ProducerRequest> requests) {
        if (requests.size() > 1) {
            send(new BoundedByteBufferSend(new MultiProducerRequest(requests)));
        } else {
            send(new BoundedByteBufferSend(requests.get(0)));
        }
    }

    private void send(BoundedByteBufferSend send) {
        synchronized (lock) {
            //TODO check send
            long start = System.currentTimeMillis();
            channel = getOrCreateChannel();
            try {
                send.writeCompletely(channel);
            } catch (IOException e) {
                disconnect();
            }
            this.sentOnChannel++;
            if (sentOnChannel >= config.getReconnectInterval()
                    || (System.currentTimeMillis() - lastConnectTime) >= config.getReconnectTimeInterval()) {
                sentOnChannel = 0;
                disconnect();
                channel = connect();
                lastConnectTime = System.currentTimeMillis();
            }
            long end = System.currentTimeMillis();
            LOGGER.info("SyncProducer send cost: " + (end - start) + " ms");
        }
    }

    private SocketChannel getOrCreateChannel() {
        if (channel == null) {
            channel = connect();
        }
        return channel;
    }

    private SocketChannel connect() {
        int connectBackoffMs = 1;
        long begin = System.currentTimeMillis();
        while (channel == null && !shutdown) {
            try {
                channel = SocketChannel.open();
                channel.configureBlocking(true);
                channel.socket().setSendBufferSize(config.getBufferSize());
                channel.socket().setKeepAlive(true);
                channel.socket().setSoTimeout(config.getSocketTimeoutMs());
                channel.connect(new InetSocketAddress(config.getHost(), config.getPort()));
            } catch (IOException e) {
                disconnect();
                long start = System.currentTimeMillis();
                if (start - begin > config.getConnectTimeoutMs()) {
                    LOGGER.error("Producer connection to " +  config.getHost() + ":" + config.getPort()
                            + " timing out after " + config.getConnectTimeoutMs() + " ms", e);
                    throw new UnableToConnectBrokerException("Unable to connect " + config.getHost() + ":" +
                            config.getPort());
                }
                try {
                    Thread.sleep(connectBackoffMs);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                LOGGER.error("Connection attempt to " + config.getHost() + ":" + config.getPort()
                        + " failed, next attempt in " + connectBackoffMs + " ms", e);
                connectBackoffMs += Math.min(10 * connectBackoffMs, MAX_CONNECT_BACKOFF_MS);
            }
        }

        return channel;
    }

    private void disconnect() {
        try {
            if (channel != null) {
                channel.close();
                channel.socket().close();
                channel = null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        disconnect();
        shutdown = true;
    }
}
