package com.jay.mykafka.message;

public class MessageAndMetadata<T> {
    private T message;
    private String topic;

    public MessageAndMetadata(T message, String topic) {
        this.message = message;
        this.topic = topic;
    }

    public T getMessage() {
        return message;
    }

    public String getTopic() {
        return topic;
    }
}
