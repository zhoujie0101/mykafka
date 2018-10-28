package com.jay.mykafka.log;

import com.jay.mykafka.message.ByteBufferMessageSet;
import com.jay.mykafka.message.FileMessageSet;

import java.io.File;

/**
 * jie.zhou
 * 2018/10/28 11:40
 */
public class LogSegment {
    private File file;
    private FileMessageSet fileMessageSet;
    private long start;
    private long firstAppendTime = 0L;

    public LogSegment(File file, FileMessageSet fileMessageSet, long start) {
        this.file = file;
        this.fileMessageSet = fileMessageSet;
        this.start = start;
    }

    public void append(ByteBufferMessageSet messageSet) {
        if (messageSet.sizeInBytes() > 0) {
            this.fileMessageSet.append(messageSet);
            updateFirstAppendTime();
        }
    }

    private void updateFirstAppendTime() {
        if (firstAppendTime == 0L) {
            firstAppendTime = System.currentTimeMillis();
        }
    }

    public File getFile() {
        return file;
    }

    public FileMessageSet getFileMessageSet() {
        return fileMessageSet;
    }

    public long getStart() {
        return start;
    }

    public long getFirstAppendTime() {
        return firstAppendTime;
    }

    public long size() {
        return fileMessageSet.getHighWaterMark();
    }
}
