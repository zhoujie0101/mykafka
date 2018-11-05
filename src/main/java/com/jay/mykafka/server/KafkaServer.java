package com.jay.mykafka.server;

import com.jay.mykafka.conf.KafkaConfig;
import com.jay.mykafka.log.LogManager;
import com.jay.mykafka.network.SocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * kafka服务
 * jie.zhou
 * 2018/10/25 11:13
 */
public class KafkaServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServer.class);

    private KafkaConfig config;
    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    private AtomicBoolean isShuttingDown = new AtomicBoolean(false);

    private SocketServer socketServer;

    private LogManager logManager;

    public KafkaServer(KafkaConfig config) {
        this.config = config;
    }

    public void startup() {
        LOGGER.info("Starting Kafka server...");

        logManager = new LogManager(config);
        KafkaRequestHandler handler = new KafkaRequestHandler(logManager);
        socketServer = new SocketServer(config.getPort(),
                config.getNumThreads(),
                config.getMonitoringPeriodSecs(),
                config.getMaxMessageSize(),
                config.getSocketSendBuffer(),
                config.getSocketReceiveBuffer(), handler);

        socketServer.startup();

        logManager.startup();

        LOGGER.info("Kafka server started");
    }

    public void awaitShutdown() {
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        boolean canShutdown = isShuttingDown.compareAndSet(false, true);
        if (!canShutdown) {
            return;
        }
        LOGGER.info("Shutting down Kafka server");
        if (socketServer != null) {
            socketServer.shutdown();
        }
        if (logManager != null) {
            logManager.close();
        }

        shutdownLatch.countDown();
        LOGGER.info("Kafka server shutdown completed");
    }
}
