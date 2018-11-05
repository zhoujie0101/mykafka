package com.jay.mykafka.serializer;

import com.jay.mykafka.message.Message;

/**
 * jie.zhou
 * 2018/10/26 16:02
 */
public interface Decoder<T> {
    T toEvent(Message message);
}
