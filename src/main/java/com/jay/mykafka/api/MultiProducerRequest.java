package com.jay.mykafka.api;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * jie.zhou
 * 2018/10/28 10:09
 */
public class MultiProducerRequest extends Request {
    private List<ProducerRequest> requests;

    public MultiProducerRequest(List<ProducerRequest> requests) {
        super(RequestKeys.MUTIL_PRODUCE);
        this.requests = requests;
    }

    @Override
    public int sizeInBytes() {
        return  2 + requests.stream().mapToInt(ProducerRequest::sizeInBytes).sum();
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        if (requests.size() > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Number of requests in MultiProducer exceeds " + Short.MAX_VALUE + ".");
        }
        buffer.putShort((short) requests.size());
        requests.forEach(req -> req.writeTo(buffer));
    }

    public static MultiProducerRequest readFrom(ByteBuffer buffer) {
        int length = buffer.getShort();
        List<ProducerRequest> requests = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            requests.add(ProducerRequest.readFrom(buffer));
        }

        return new MultiProducerRequest(requests);
    }
}
