package com.jay.mykafka.serializer;

import com.jay.mykafka.message.Message;

/**
 * jie.zhou
 * 2018/10/26 21:31
 */
public class StringEncoder implements Encoder<String> {

    @Override
    public Message toMessage(String m) {
        return new Message(m.getBytes());
    }
}
