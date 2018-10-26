package com.jay.mykafka.api;

import java.nio.ByteBuffer;

/**
 * jie.zhou
 * 2018/10/26 21:18
 */
public abstract class Request {
    protected short id;

    public Request(short id) {
        this.id = id;
    }

    public abstract int sizeInBytes();

    public abstract void writeTo(ByteBuffer buffer);
}
