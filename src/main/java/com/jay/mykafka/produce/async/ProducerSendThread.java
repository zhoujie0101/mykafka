package com.jay.mykafka.produce.async;

import com.jay.mykafka.common.IllegalQueueStateException;
import com.jay.mykafka.produce.ProducerPool;
import com.jay.mykafka.produce.SyncProducer;
import com.jay.mykafka.serializer.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * jie.zhou
 * 2018/11/5 15:05
 */
public class ProducerSendThread<T> extends Thread {
    public static final Logger LOGGER = LoggerFactory.getLogger(ProducerSendThread.class);

    private String threadName;
    private BlockingQueue<QueueItem<T>> queue;
    private Encoder<T> serializer;
    private SyncProducer underlyingProducer;
    private EventHandler<T> eventHandler;
    private CallbackHandler<T> cbkHandler;
    private long queueTime;
    private int batchSize;
    private Object shutdownCommand;
    private CountDownLatch shutdownLatch = new CountDownLatch(1);


    public ProducerSendThread(String threadName, BlockingQueue<QueueItem<T>> queue, Encoder<T> serializer,
                                  SyncProducer syncProducer, EventHandler<T> eventHandler, CallbackHandler<T> ckHandler,
                                  int queueTime, int batchSize, Object shutdownCommand) {
        super(threadName);
        this.threadName = threadName;
        this.queue = queue;
        this.serializer = serializer;
        this.underlyingProducer = syncProducer;
        this.eventHandler = eventHandler;
        this.cbkHandler = ckHandler;
        this.queueTime = queueTime;
        this.batchSize = batchSize;
        this.shutdownCommand = shutdownCommand;
    }

    @Override
    public void run() {
        try {
            List<QueueItem<T>> remainingEvents = processEvents();
            if (remainingEvents.size() > 0) {
                tryToHandle(remainingEvents);
            }
        } catch (Exception e) {
            LOGGER.error("Error in sending events", e);
        } finally {
            shutdownLatch.countDown();
        }
    }

    private List<QueueItem<T>> processEvents() throws InterruptedException {
        long lastSend = System.currentTimeMillis();
        List<QueueItem<T>> events = new ArrayList<>();
        while (true) {
            QueueItem<T> event = queue.poll(Math.max(0, lastSend + queueTime - System.currentTimeMillis()),
                    TimeUnit.MILLISECONDS);
            if (event == shutdownCommand) {
                break;
            }
            boolean expired = false;
            if (event != null) {
                events.add(event);
            } else {
                expired = true;
            }
            boolean full = events.size() > batchSize;
            if (expired || full) {
                tryToHandle(events);
                lastSend = System.currentTimeMillis();
                events = new ArrayList<>();
            }
        }

        if (queue.size() > 0) {
            throw new IllegalQueueStateException("Invalid queue state! After queue shutdown, " + queue.size()
                    + " remaining items in the queue");
        }

        return events;
    }

    private void tryToHandle(List<QueueItem<T>> events) {

    }

    public void shutdown() {
        eventHandler.close();
    }

    public void awaitShutdown() {
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
