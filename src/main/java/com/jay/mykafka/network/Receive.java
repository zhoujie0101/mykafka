package com.jay.mykafka.network;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * jie.zhou
 * 2018/10/25 17:22
 */
public interface Receive extends Transmission {
    ByteBuffer buffer();

    int readFrom(ReadableByteChannel channel);

    default int readCompletely(ReadableByteChannel channel) {
        int read = 0;
        while (!complete()) {
            read = readFrom(channel);
        }

        return read;
    }
}
