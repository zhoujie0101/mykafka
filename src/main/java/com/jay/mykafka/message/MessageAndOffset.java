package com.jay.mykafka.message;

/**
 * jie.zhou
 * 2018/10/26 18:38
 */
public class MessageAndOffset {
    private Message message;
    private long offset;

    public MessageAndOffset(Message message, long offset) {
        this.message = message;
        this.offset = offset;
    }

    public Message getMessage() {
        return message;
    }

    public long getOffset() {
        return offset;
    }
}
