package com.jay.mykafka.api;

import com.jay.mykafka.message.ByteBufferMessageSet;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * jie.zhou
 * 2018/10/26 21:17
 */
public class ProducerRequest extends Request {
    private String topic;
    private int partition;
    private ByteBufferMessageSet messages;


    public ProducerRequest(String topic, int partition, ByteBufferMessageSet messages) {
        super(RequestKeys.PRODUCE);
        this.topic = topic;
        this.partition = partition;
        this.messages = messages;
    }

    @Override
    public int sizeInBytes() {
        return 2  //topic
                + topic.length()
                + 4
                + 4
                + (int) messages.sizeInBytes();
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes(StandardCharsets.UTF_8));
        buffer.putInt(partition);
        buffer.putInt(messages.serialized().limit());
        buffer.put(messages.serialized());
        buffer.rewind();
    }
}
