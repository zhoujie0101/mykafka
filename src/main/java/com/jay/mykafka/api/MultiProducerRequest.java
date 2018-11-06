package com.jay.mykafka.api;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * jie.zhou
 * 2018/10/28 10:09
 */
public class MultiProducerRequest extends Request {
    private List<ProducerRequest> producers;

    public MultiProducerRequest(List<ProducerRequest> producers) {
        super(RequestKeys.MUTIL_PRODUCE);
        this.producers = producers;
    }

    @Override
    public int sizeInBytes() {
        return  2 + producers.stream().mapToInt(ProducerRequest::sizeInBytes).sum();
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        if (producers.size() > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Number of producers in MultiProducer exceeds " + Short.MAX_VALUE + ".");
        }
        buffer.putShort((short) producers.size());
        producers.forEach(req -> req.writeTo(buffer));
    }

    public static MultiProducerRequest readFrom(ByteBuffer buffer) {
        short length = buffer.getShort();
        List<ProducerRequest> requests = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            requests.add(ProducerRequest.readFrom(buffer));
        }

        return new MultiProducerRequest(requests);
    }

    public List<ProducerRequest> getProducers() {
        return producers;
    }
}
