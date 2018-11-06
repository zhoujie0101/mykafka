package com.jay.mykafka.message;

import com.jay.mykafka.util.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * jie.zhou
 * 2018/10/28 11:48
 */
public class FileMessageSet extends MessageSet {
    private FileChannel channel;
    private boolean mutable;
    private long offset;
    private long limit;
    private AtomicLong size = new AtomicLong();
    private AtomicLong highWaterMark = new AtomicLong();

    public FileMessageSet(File file, boolean mutable) {
        this(Utils.openChannel(file, mutable), mutable);
    }

    public FileMessageSet(FileChannel channel, boolean mutable) {
        this(channel, mutable, 0, Long.MAX_VALUE);
    }

    public FileMessageSet(FileChannel channel, boolean mutable, long offset, long limit) {
        if(limit < Long.MAX_VALUE || offset > 0) {
            throw new IllegalArgumentException("Attempt to open a mutable message set with a view or offset, which is not allowed.");
        }

        this.channel = channel;
        this.mutable = mutable;
        this.offset = offset;
        this.limit = limit;
        try {
            if (mutable) {
                this.size.set(channel.size());
                highWaterMark.set(sizeInBytes());
                channel.position(channel.size());
            } else {
                this.size.set(Math.min(channel.size(), limit) - offset);
                this.highWaterMark.set(sizeInBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public MessageSet read(long readOffset, long readSize) {
        return new FileMessageSet(channel, mutable, offset + readOffset,
                Math.min(offset + readOffset + readSize, getHighWaterMark()));
    }

    @Override
    public long writeTo(GatheringByteChannel destChannel, long writeOffset, long size) {
        try {
            return channel.transferTo(offset + writeOffset, Math.min(size, sizeInBytes()), destChannel);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public void append(MessageSet messageSet) {
        checkMutable();
        long written = 0L;
        while (written < messageSet.sizeInBytes()) {
            written += messageSet.writeTo(channel, 0, messageSet.sizeInBytes());
        }
        this.size.getAndAdd(written);
    }

    private void checkMutable() {
        if (!this.mutable) {
            throw new IllegalStateException("Attempt to invoke mutation on immutable message set.");
        }
    }

    public void flush() {
        checkMutable();
        try {
            channel.force(true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        highWaterMark.set(sizeInBytes());
    }

    public void close() {
        if (this.mutable) {
            flush();
        }
        try {
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<MessageAndOffset> iterator() {
        return null;
    }

    @Override
    public long sizeInBytes() {
        return size.get();
    }

    public long getHighWaterMark() {
        return highWaterMark.get();
    }

    public long getSize() {
        return size.get();
    }
}
