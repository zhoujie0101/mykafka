package com.jay.mykafka.util;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * jie.zhou
 * 2018/10/25 14:29
 */
public class KafkaScheduler {
    private int numThreads;
    private String threadName;
    private boolean daemon;
    private AtomicLong threadId = new AtomicLong(0);
    private ScheduledThreadPoolExecutor executor;

    public KafkaScheduler(int numThreads, String threadName, boolean daemon) {
        this.numThreads = numThreads;
        this.threadName = threadName;
        this.daemon = daemon;
        executor = new ScheduledThreadPoolExecutor(numThreads, r -> {
            Thread t = new Thread(r, threadName + threadId.getAndIncrement());
            t.setDaemon(daemon);
            return t;
        });
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    public ScheduledFuture<?> scheduleWithRate(Function<Void, Void> func, long delayMs, long periodMs) {
        return executor.scheduleAtFixedRate(() -> func.apply(null), delayMs, periodMs, TimeUnit.MILLISECONDS);
    }
}
