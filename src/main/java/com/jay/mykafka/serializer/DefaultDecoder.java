package com.jay.mykafka.serializer;

import com.jay.mykafka.message.Message;

/**
 * jie.zhou
 * 2018/11/5 11:43
 */
public class DefaultDecoder implements Decoder<Message> {

    @Override
    public Message toEvent(Message message) {
        return message;
    }
}
