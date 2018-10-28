package com.jay.mykafka.log;

import com.jay.mykafka.common.InvalidFileException;
import com.jay.mykafka.message.ByteBufferMessageSet;
import com.jay.mykafka.message.FileMessageSet;
import com.jay.mykafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * jie.zhou
 * 2018/10/25 14:57
 */
public class Log {
    public static final Logger LOGGER = LoggerFactory.getLogger(Log.class);
    public static final String FILE_SUFFIX = ".kafka";
    private Object lock = new Object();
    private File dir;
    private String name;
    private CopyOnWriteArrayList<LogSegment> segments;
    /**
     * The current number of unflushed messages appended to the write
     */
    private AtomicLong unflushed = new AtomicLong(0);
    private AtomicLong lastFlushedTime = new AtomicLong(System.currentTimeMillis());
    private long maxLogFileSize;
    private int maxMessageSize;
    private int flushInterval;
    private long rollIntervalMs;

    public Log(File dir, long maxLogFileSize, int maxMessageSize, int flushInterval, long rollIntervalMs) {
        this.dir = dir;
        this.name = dir.getName();
        this.maxLogFileSize = maxLogFileSize;
        this.maxMessageSize = maxMessageSize;
        this.flushInterval = flushInterval;
        this.rollIntervalMs = rollIntervalMs;

        segments = loadSegments();
    }

    private CopyOnWriteArrayList<LogSegment> loadSegments() {
        List<LogSegment> segments = new ArrayList<>();
        File[] files = dir.listFiles();
        if (files != null) {
            for (int i = 0; i < files.length && files[i].toString().endsWith(FILE_SUFFIX); i++) {
                if (files[i].canRead()) {
                    throw new InvalidFileException("Could not read file " +  files[i]);
                }
                String filename = files[i].getName();
                long start = Long.parseLong(filename.substring(0, filename.length() - FILE_SUFFIX.length()));
                FileMessageSet messageSet = new FileMessageSet(files[i], false);
                segments.add(new LogSegment(files[i], messageSet, start));
            }
        }

        if (segments.size() == 0) {
            File file = new File(dir, nameFromOffset(0));
            FileMessageSet messageSet = new FileMessageSet(file, true);
            segments.add(new LogSegment(file, messageSet, 0));
        } else {
            //sort ascending
            segments.sort(Comparator.comparingLong(LogSegment::getStart));
            LogSegment last = segments.get(segments.size() - 1);
            last.getFileMessageSet().close();
            FileMessageSet messageSet = new FileMessageSet(last.getFile(), true);
            segments.add(new LogSegment(last.getFile(), messageSet, last.getStart()));
        }

        return new CopyOnWriteArrayList<>(segments);
    }

    public void append(ByteBufferMessageSet messages) {
        synchronized (lock) {
            int numMessages = 0;
            for (MessageAndOffset mao : messages) {
                numMessages += 1;
            }
            LogSegment segment = segments.get(segments.size() - 1);
            maybeRoll(segment);
            segment = segments.get(segments.size() - 1);
            segment.append(messages);
            maybeFlush(numMessages);
        }
    }

    private void maybeRoll(LogSegment segment) {
        if (segment.getFileMessageSet().sizeInBytes() > maxLogFileSize || (segment.getFirstAppendTime() != 0
                && segment.getFirstAppendTime() - System.currentTimeMillis() > rollIntervalMs)) {
            roll();
        }
    }

    private void roll() {
        synchronized (lock) {
            long newOffset = nextAppendOffset();
            File newFile = new File(dir, nameFromOffset(newOffset));
            if (newFile.exists()) {
                LOGGER.warn("newly rolled logsegment " + newFile.getName() + " already exists; deleting it first");
            }
            segments.add(new LogSegment(newFile, new FileMessageSet(newFile, true), newOffset));
        }
    }

    private long nextAppendOffset() {
        flush();
        LogSegment segment = segments.get(segments.size() - 1);
        return segment.getStart() + segment.size();
    }

    private void maybeFlush(int numMessages) {
        if (unflushed.get() - numMessages > flushInterval) {
            flush();
        }
    }

    private void flush() {
        if (unflushed.get() == 0) {
            return;
        }
        synchronized (lock) {
            unflushed.getAndIncrement();
            lastFlushedTime.set(System.currentTimeMillis());
            segments.get(segments.size() - 1).getFileMessageSet().flush();
        }
    }

    public void close() {
        for (LogSegment segment : segments) {
            segment.getFileMessageSet().close();
        }
    }

    public static String nameFromOffset(long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset) + FILE_SUFFIX;
    }
}
