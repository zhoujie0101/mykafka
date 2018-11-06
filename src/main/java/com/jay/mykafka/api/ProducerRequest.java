package com.jay.mykafka.api;

import com.jay.mykafka.message.ByteBufferMessageSet;

import java.io.UnsupportedEncodingException;
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
    public static final int RANDOM_PARTITION = -1;

    public ProducerRequest(String topic, int partition, ByteBufferMessageSet messages) {
        super(RequestKeys.PRODUCE);
        this.topic = topic;
        this.partition = partition;
        this.messages = messages;
    }

    @Override
    public int sizeInBytes() {
        return 2  //topic length
                + topic.length()  //topic
                + 4  //partition
                + 4  //message size
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

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public ByteBufferMessageSet getMessages() {
        return messages;
    }

    public static ProducerRequest readFrom(ByteBuffer buffer) {
        short topicLength = buffer.getShort();
        byte[] bytes = new byte[topicLength];
        buffer.get(bytes);
        String topic = null;
        try {
            topic = new String(bytes, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        int partition = buffer.getInt();
        int messageSize = buffer.getInt();
        ByteBuffer message = buffer.slice();
        message.limit(messageSize);
        buffer.position(buffer.position() + messageSize);

        return new ProducerRequest(topic, partition, new ByteBufferMessageSet(message));
    }
}
