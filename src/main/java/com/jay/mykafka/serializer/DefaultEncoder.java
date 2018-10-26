package com.jay.mykafka.serializer;

import com.jay.mykafka.message.Message;

/**
 * jie.zhou
 * 2018/10/26 21:30
 */
public class DefaultEncoder implements Encoder<Message> {
    @Override
    public Message toMessage(Message m) {
        return m;
    }
}
