package com.jay.mykafka.network;

import java.nio.channels.GatheringByteChannel;

/**
 * jie.zhou
 * 2018/10/25 17:24
 */
public interface Send extends Transmission {
    int writeTo(GatheringByteChannel channel);

    default int writeCompletely(GatheringByteChannel channel) {
        int written = 0;
        while (!complete()) {
            written = writeTo(channel);
        }

        return written;
    }
}
