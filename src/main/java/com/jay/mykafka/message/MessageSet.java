package com.jay.mykafka.message;

import java.nio.channels.GatheringByteChannel;

/**
 * jie.zhou
 * 2018/10/26 18:38
 */
public abstract class MessageSet implements Iterable<MessageAndOffset> {
    public abstract long writeTo(GatheringByteChannel channel, long offset, long maxSize);

    public abstract long sizeInBytes();

    public void valid() {
        for (MessageAndOffset mao : this) {
            if (!mao.getMessage().isValid()) {
                throw new InvalidMessageException();
            }
        }
    }
}
