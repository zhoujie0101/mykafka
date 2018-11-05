package com.jay.mykafka.message;

import com.jay.mykafka.common.InvalidMessageException;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.List;

/**
 * A set of messages. A message set has a fixed serialized form, though the container
 * for the bytes could be either in-memory or on disk. A The format of each message is
 * as follows:
 * 4 byte size containing an integer N
 * N message bytes as described in the message class
 * jie.zhou
 * 2018/10/26 18:38
 */
public abstract class MessageSet implements Iterable<MessageAndOffset> {
    public static final int LOG_OVERHEAD = 4;

    public static ByteBuffer createByteBuffer(List<Message> messages) {
        ByteBuffer buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages));
        for (Message m : messages) {
            m.serializedTo(buffer);
        }
        buffer.rewind();

        return buffer;
    }

    public static int messageSetSize(List<Message> messages) {
        return messages.stream().mapToInt(MessageSet::entrySize).sum();
    }

    public static int entrySize(Message m) {
        return LOG_OVERHEAD + m.size();
    }

    /** Write the messages in this set to the given channel starting at the given offset byte.
     * Less than the complete amount may be written, but no more than maxSize can be. The number
     * of bytes written is returned */
    public abstract long writeTo(GatheringByteChannel channel, long offset, long maxSize);

    /**
     * Gives the total size of this message set in bytes
     */
    public abstract long sizeInBytes();

    /**
     * Validate the checksum of all the messages in the set. Throws an InvalidMessageException if the checksum doesn't
     * match the payload for any message.
     */
    public void valid() {
        for (MessageAndOffset mao : this) {
            if (!mao.getMessage().isValid()) {
                throw new InvalidMessageException("invalid message");
            }
        }
    }
}
