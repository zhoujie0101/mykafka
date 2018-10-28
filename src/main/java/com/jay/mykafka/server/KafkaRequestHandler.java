package com.jay.mykafka.server;

import com.jay.mykafka.api.ProducerRequest;
import com.jay.mykafka.api.RequestKeys;
import com.jay.mykafka.log.LogManager;
import com.jay.mykafka.network.Receive;
import com.jay.mykafka.network.Send;

import java.nio.channels.SelectionKey;
import java.util.Optional;

/**
 * jie.zhou
 * 2018/10/26 10:06
 */
public class KafkaRequestHandler {
    private LogManager logManager;

    public KafkaRequestHandler(LogManager logManager) {
        this.logManager = logManager;
    }

    public Send handleRequest(SelectionKey key, Receive receive) {
        int requestTypeId = receive.buffer().getShort();
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
        int partition = request.getPartition();
        if (partition == -1) {
            partition = logManager.choosePartition(request.getTopic());
        }
        try {
            logManager.getOrCreateLog(request.getTopic(), partition).append(request.getMessages());
        } catch (Exception e) {

        }

        return null;
    }

    private Send handleFetchRequest(Receive receive) {
        return null;
    }

    private Send handleMultiFetchRequest(Receive receive) {
        return null;
    }

    private Send handleMultiProduceRequest(Receive receive) {
        return null;
    }

    private Send handleOffsetRequest(Receive receive) {
        return null;
    }
}
