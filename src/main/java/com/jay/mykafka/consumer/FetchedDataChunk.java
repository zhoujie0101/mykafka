package com.jay.mykafka.consumer;

import com.jay.mykafka.message.ByteBufferMessageSet;

/**
 * jie.zhou
 * 2018/11/6 11:02
 */
public class FetchedDataChunk {
    private ByteBufferMessageSet messages;
    private PartitionTopicInfo info;
    private long fetchOffset;

    public FetchedDataChunk(ByteBufferMessageSet messages, PartitionTopicInfo info, long fetchOffset) {
        this.messages = messages;
        this.info = info;
        this.fetchOffset = fetchOffset;
    }

    public ByteBufferMessageSet getMessages() {
        return messages;
    }

    public PartitionTopicInfo getInfo() {
        return info;
    }

    public long getFetchOffset() {
        return fetchOffset;
    }
}
