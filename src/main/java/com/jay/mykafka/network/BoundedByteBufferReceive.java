package com.jay.mykafka.network;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * jie.zhou
 * 2018/10/25 17:34
 */
public class BoundedByteBufferReceive implements Receive {
    private int maxRequestSize;

    public BoundedByteBufferReceive(int maxRequestSize) {
        this.maxRequestSize = maxRequestSize;
    }

    @Override
    public ByteBuffer buffer() {
        return null;
    }

    @Override
    public int readFrom(ReadableByteChannel channel) {
        return 0;
    }

    @Override
    public boolean complete() {
        return false;
    }
}
