package com.jay.mykafka.serializer;

import com.jay.mykafka.message.Message;

import java.nio.ByteBuffer;

/**
 * jie.zhou
 * 2018/11/5 11:44
 */
public class StringDecoder implements Decoder<String> {
    @Override
    public String toEvent(Message message) {
        ByteBuffer buf = message.payload();
        byte[] bytes = new byte[buf.remaining()];

        return new String(bytes);
    }
}
