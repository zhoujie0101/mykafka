package com.jay.mykafka.message;

import com.jay.mykafka.common.ErrorMapping;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.List;

/**
 * jie.zhou
 * 2018/10/26 20:38
 */
public class ByteBufferMessageSet extends MessageSet {
    private ByteBuffer buffer;
    private long initialOffset = 0L;
    private int errorCode = ErrorMapping.NO_ERROR;

    private long shallowValidByteCount = -1L;

    public ByteBufferMessageSet(List<Message> messages) {
        this(MessageSet.createByteBuffer(messages), 0L, ErrorMapping.NO_ERROR);
    }

    public ByteBufferMessageSet(ByteBuffer buffer, long initialOffset, int errorCode) {
        this.buffer = buffer;
        this.initialOffset = initialOffset;
        this.errorCode = errorCode;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public long getInitialOffset() {
        return initialOffset;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public ByteBuffer serialized() {
        return buffer;
    }

    public long validBytes() {
        return shallowValidBytes();
    }

    private long shallowValidBytes() {
        if (shallowValidByteCount < 0) {
            Iterator<MessageAndOffset> iter = this.internalIterator(true);
            while (iter.hasNext()) {
                shallowValidByteCount = iter.next().getOffset();
            }
        }

        if (shallowValidByteCount < initialOffset) {
            return 0L;
        } else {
            return shallowValidByteCount - initialOffset;
        }
    }

    protected Iterator<MessageAndOffset> internalIterator(boolean isShallow) {
        return null;
    }

    @Override
    public long writeTo(GatheringByteChannel channel, long offset, long maxSize) {
        return 0;
    }

    @Override
    public long sizeInBytes() {
        return 0;
    }

    @Override
    public Iterator<MessageAndOffset> iterator() {
        return null;
    }
}
