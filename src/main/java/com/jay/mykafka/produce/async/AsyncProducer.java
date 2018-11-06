package com.jay.mykafka.produce.async;

import com.jay.mykafka.api.ProducerRequest;
import com.jay.mykafka.produce.SyncProducer;
import com.jay.mykafka.serializer.Encoder;
import com.jay.mykafka.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * jie.zhou
 * 2018/10/26 16:05
 */
public class AsyncProducer<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncProducer.class);

    private AsyncProducerConfig config;
    private SyncProducer syncProducer;
    private Encoder<T> encoder;
    private EventHandler<T> eventHandler;
    private Properties eventHandlerProps;
    private CallbackHandler<T> ckHandler;
    private Properties ckHandlerProps;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private BlockingQueue<QueueItem<T>> queue;
    private Random rand = new Random();
    private int asyncProducerId = rand.nextInt();
    private Object shutdown = new Object();
    private ProducerSendThread sendThread;

    public AsyncProducer(AsyncProducerConfig config) {
        this(config, new SyncProducer(config.getSyncProducerConfig()), Utils.getObject(config.getSerializerClass()),
                Utils.getObject(config.getEventHandler()), config.getEventHandlerProps(),
                Utils.getObject(config.getCkHanler()), config.getCkHandlerProps());
    }

    public AsyncProducer(AsyncProducerConfig config, SyncProducer syncProducer, Encoder<T> encoder,
                         EventHandler<T> eventHandler, Properties eventHandlerProps,
                         CallbackHandler<T> ckHandler, Properties ckHandlerProps) {
        this.config = config;
        this.syncProducer = syncProducer;
        this.encoder = encoder;
        this.eventHandler = eventHandler;
        this.eventHandlerProps = eventHandlerProps;
        this.ckHandler = ckHandler;
        this.ckHandlerProps = ckHandlerProps;
        if (this.eventHandler != null) {
            this.eventHandler.init(eventHandlerProps);
        } else {
            this.eventHandler = new DefaultEventHandler<>(config, ckHandler);
        }
        if (this.ckHandler != null) {
            this.ckHandler.init(ckHandlerProps);
        }
        queue = new LinkedBlockingQueue<>(config.getQueueSize());
        sendThread = new ProducerSendThread<>("ProducerSendThread-" + asyncProducerId, queue, encoder, this.syncProducer,
                this.eventHandler, this.ckHandler, config.getQueueTime(), config.getBatchSize(), shutdown);
        sendThread.setDaemon(false);
    }

    public void send(String topic, T event) {
        send(topic, ProducerRequest.RANDOM_PARTITION, event);
    }

    public void send(String topic, int partition, T event) {
        if (closed.get()) {
            throw new QueueClosedException("Attemp to add event to a closed queue.");
        }
        QueueItem<T> item = new QueueItem<>(topic, partition, event);
        if (ckHandler != null) {
            ckHandler.beforeEnqueue(item);
        }

        boolean added = false;
        if (config.getEnqueueTimeoutMs() == 0) {
            added = queue.offer(item);
        } else {
            try {
                if (config.getEnqueueTimeoutMs() < 0) {
                    queue.put(item);
                    added = true;
                } else {
                    added = queue.offer(item, config.getEnqueueTimeoutMs(), TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                LOGGER.error(getClass().getSimpleName() + " interrupted during enqueue of event " + event.toString());
            }
        }

        if (ckHandler != null) {
            ckHandler.afterEnqueue(item, added);
        }

        if (!added) {
            LOGGER.error("Event queue is full of unsent messages, could not send event: " + event.toString());
            throw new QueueFullException("Event queue is full of unsent messages, could not send event: "
                    + event.toString());
        }
    }

    public void start() {
        sendThread.start();
    }

    public void close() {
        if (ckHandler != null) {
            ckHandler.close();
        }
        closed.set(true);
        try {
            queue.put(new QueueItem(null, -1, shutdown));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        sendThread.shutdown();
        sendThread.awaitShutdown();
        syncProducer.close();
    }
}
