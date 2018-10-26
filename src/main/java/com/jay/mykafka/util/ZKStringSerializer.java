package com.jay.mykafka.util;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

/**
 * jie.zhou
 * 2018/10/26 10:54
 */
public class ZKStringSerializer implements ZkSerializer {
    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
        return ((String) data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        try {
            return new String(bytes, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }
}
