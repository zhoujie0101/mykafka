package com.jay.mykafka.serializer;

import com.jay.mykafka.message.Message;

/**
 * jie.zhou
 * 2018/10/26 16:01
 */
public interface Encoder<T> {
    Message toMessage(T d);
}
