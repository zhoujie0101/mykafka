package com.jay.mykafka.server;

import com.jay.mykafka.api.MultiProducerRequest;
import com.jay.mykafka.api.OffsetRequest;
import com.jay.mykafka.api.OffsetRequest.OffsetArraySend;
import com.jay.mykafka.api.ProducerRequest;
import com.jay.mykafka.api.RequestKeys;
import com.jay.mykafka.log.LogManager;
import com.jay.mykafka.network.Receive;
import com.jay.mykafka.network.Send;

/**
 * jie.zhou
 * 2018/10/26 10:06
 */
public class KafkaRequestHandler {
    private LogManager logManager;

    public KafkaRequestHandler(LogManager logManager) {
        this.logManager = logManager;
    }

    public Send handleRequest(short requestTypeId, Receive receive) {
        switch (requestTypeId) {
            case RequestKeys.PRODUCE:
                return handleProduceRequest(receive);
            case RequestKeys.FETCH:
                return handleFetchRequest(receive);
            case RequestKeys.MULTI_FETCH:
                return handleMultiFetchRequest(receive);
            case RequestKeys.MUTIL_PRODUCE:
                return handleMultiProduceRequest(receive);
            case RequestKeys.OFFSETS:
                return handleOffsetRequest(receive);
            default:
                throw new IllegalStateException("No mapping found for handler id " + requestTypeId);
        }
    }

    private Send handleProduceRequest(Receive receive) {
        ProducerRequest request = ProducerRequest.readFrom(receive.buffer());
        handleProduceRequest(request);

        return null;
    }

    private Send handleMultiProduceRequest(Receive receive) {
        MultiProducerRequest request = MultiProducerRequest.readFrom(receive.buffer());
        request.getProducers().forEach(this::handleProduceRequest);

        return null;
    }

    private void handleProduceRequest(ProducerRequest producerRequest) {
        int partition = producerRequest.getPartition();
        if (partition == ProducerRequest.RANDOM_PARTITION) {
            partition = logManager.chooseRandomPartition(producerRequest.getTopic());
        }
        try {
            logManager.getOrCreateLog(producerRequest.getTopic(), partition).append(producerRequest.getMessages());
        } catch (Exception e) {
            //TODO handle error
        }
    }

    private Send handleFetchRequest(Receive receive) {
        return null;
    }

    private Send handleMultiFetchRequest(Receive receive) {
        return null;
    }

    private Send handleOffsetRequest(Receive receive) {
        OffsetRequest request = OffsetRequest.readFrom(receive.buffer());
        long[] offsets = logManager.getOffsets(request);

        return new OffsetArraySend(offsets);
    }

    private Send readMessageSet(OffsetRequest request) {
        return null;
    }
}
