package com.jay.mykafka.network;

import com.jay.mykafka.common.InvalidRequestException;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * jie.zhou
 * 2018/10/25 17:34
 */
public class BoundedByteBufferReceive implements Receive {
    private int maxSize;
    private ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
    private ByteBuffer contentBuffer;
    private boolean complete;

    public BoundedByteBufferReceive() {
        this(Integer.MAX_VALUE);
    }

    public BoundedByteBufferReceive(int maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public ByteBuffer buffer() {
        expectComplete();

        return contentBuffer;
    }

    @Override
    public int readFrom(ReadableByteChannel channel) throws IOException {
        expectIncomplete();
        int read = 0;
        if (sizeBuffer.hasRemaining()) {
            read += channel.read(sizeBuffer);
            if (read == -1) {
                throw new EOFException();
            }
        }
        if (!sizeBuffer.hasRemaining() && contentBuffer == null) {
            sizeBuffer.rewind();
            int size = sizeBuffer.getInt();
            if (size <= 0) {
                throw new InvalidRequestException(size + " is not a valid request size.");
            } else if (size >= maxSize) {
                throw new InvalidRequestException("Request of length " + size
                        + " is not valid, it is larger than the maximum size of " + maxSize + " bytes.");
            }
            contentBuffer = ByteBuffer.allocate(size);
        }
        if (contentBuffer != null) {
            read += channel.read(contentBuffer);
            if (!contentBuffer.hasRemaining()) {
                complete = true;
                contentBuffer.rewind();
            }
        }

        return read;
    }

    @Override
    public boolean complete() {
        return complete;
    }
}
