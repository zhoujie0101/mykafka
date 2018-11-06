package com.jay.mykafka.log;

import com.jay.mykafka.api.OffsetRequest;
import com.jay.mykafka.common.InvalidFileException;
import com.jay.mykafka.common.InvalidMessageSizeException;
import com.jay.mykafka.message.ByteBufferMessageSet;
import com.jay.mykafka.message.FileMessageSet;
import com.jay.mykafka.message.MessageAndOffset;
import com.jay.mykafka.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * kafka log
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

    public static long[] getEmptyOffsets(OffsetRequest request) {
        if (request.getTime() == OffsetRequest.LATEST_TIME || request.getTime() == OffsetRequest.EARLIEST_TIME) {
            return new long[]{0};
        } else {
            return new long[1];
        }
    }

    private CopyOnWriteArrayList<LogSegment> loadSegments() {
        List<LogSegment> segments = new ArrayList<>();
        File[] files = dir.listFiles();
        if (files != null) {
            for (int i = 0; i < files.length && files[i].toString().endsWith(FILE_SUFFIX); i++) {
                if (!files[i].canRead()) {
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
            //TODO need to validate segment
            LogSegment last = segments.get(segments.size() - 1);
            last.getFileMessageSet().close();
            FileMessageSet messageSet = new FileMessageSet(last.getFile(), true);
            segments.add(new LogSegment(last.getFile(), messageSet, last.getStart()));
        }

        return new CopyOnWriteArrayList<>(segments);
    }

    public void append(ByteBufferMessageSet messages) {
        int numMessages = 0;
        for (MessageAndOffset mao : messages) {
            numMessages += 1;
        }

        ByteBufferMessageSet validMessages = checkMessage(messages);

        synchronized (lock) {
            LogSegment segment = segments.get(segments.size() - 1);
            maybeRoll(segment);
            segment = segments.get(segments.size() - 1);
            segment.append(validMessages);
            maybeFlush(numMessages);
        }
    }

    private ByteBufferMessageSet checkMessage(ByteBufferMessageSet messages) {
        ByteBuffer validBuf = messages.getBuffer().duplicate();
        long validBytes = messages.validBytes();
        if (validBytes > Integer.MAX_VALUE || validBytes < 0) {
            throw new InvalidMessageSizeException("Illegal length of message set " + validBytes +
                    " Message set cannot be appended to log. Possible causes are corrupted produce requests");
        }
        validBuf.limit((int) validBytes);

        return new ByteBufferMessageSet(validBuf);
    }

    /**
     * Roll the log over if necessary
     */
    private void maybeRoll(LogSegment segment) {
        if (segment.getFileMessageSet().sizeInBytes() > maxLogFileSize || (segment.getFirstAppendTime() != 0
                && (System.currentTimeMillis() - segment.getFirstAppendTime()) > rollIntervalMs)) {
            roll();
        }
    }

    /**
     * Create a new segment and make it active
     */
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

    /**
     * Flush the log if necessary
     */
    private void maybeFlush(int numMessages) {
        if (unflushed.addAndGet(numMessages) > flushInterval) {
            flush();
        }
    }

    /**
     * Flush this log file to the physical disk
     */
    private void flush() {
        if (unflushed.get() == 0) {
            return;
        }
        synchronized (lock) {
            segments.get(segments.size() - 1).getFileMessageSet().flush();
            unflushed.set(0L);
            lastFlushedTime.set(System.currentTimeMillis());
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

    public long[] getOffsetsBefore(OffsetRequest request) {
        List<Tuple<Long, Long>> offsetTimeList;
        int len;
        if (segments.get(segments.size() - 1).size() > 0) {
            len = segments.size() + 1;
            offsetTimeList = new ArrayList<>(len);
        } else {
            len = segments.size();
            offsetTimeList = new ArrayList<>(len);
        }

        for (int i = 0; i < segments.size(); i++) {
            offsetTimeList.add(new Tuple<>(segments.get(i).getStart(), segments.get(i).getFile().lastModified()));
        }
        if (len == segments.size() + 1) {
            LogSegment segment = segments.get(segments.size() - 1);
            offsetTimeList.add(new Tuple<>(segment.getStart() + segment.getFileMessageSet().getHighWaterMark(),
                    System.currentTimeMillis()));
        }

        int startIndex;
        if (request.getTime() == OffsetRequest.LATEST_TIME) {
            startIndex = offsetTimeList.size() - 1;
        } else if (request.getTime() == OffsetRequest.EARLIEST_TIME) {
            startIndex = 0;
        } else {
            startIndex = offsetTimeList.size() - 1;
            boolean found = false;
            while (startIndex >= 0 && !found) {
                if (offsetTimeList.get(startIndex).getSecond() <= request.getTime()) {
                    found = true;
                } else {
                    startIndex--;
                }
            }
        }

        int retSize = Math.min(request.getMaxNumOffset(), startIndex + 1);
        long[] ret = new long[retSize];
        for (int i = 0; i < retSize; i++) {
            ret[i] = offsetTimeList.get(startIndex).getFirst();
            startIndex--;
        }

        return ret;
    }
}
