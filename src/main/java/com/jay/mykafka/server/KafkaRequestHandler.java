package com.jay.mykafka.server;

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

    public Send handleRequest(SelectionKey key, Receive request) {
        int requestTypeId = request.buffer().getShort();
        switch (requestTypeId) {
            case RequestKeys.PRODUCE:
                return handleProduceRequest(request);
            case RequestKeys.FETCH:
                return handleFetchRequest(request);
            case RequestKeys.MULTI_FETCH:
                return handleMultiFetchRequest(request);
            case RequestKeys.MUTIL_PRODUCE:
                return handleMultiProduceRequest(request);
            case RequestKeys.OFFSETS:
                return handleOffsetRequest(request);
            default:
                throw new IllegalStateException("No mapping found for handler id " + requestTypeId);
        }
    }

    private Send handleProduceRequest(Receive request) {
        return null;
    }

    private Send handleFetchRequest(Receive request) {
        return null;
    }

    private Send handleMultiFetchRequest(Receive request) {
        return null;
    }

    private Send handleMultiProduceRequest(Receive request) {
        return null;
    }

    private Send handleOffsetRequest(Receive request) {
        return null;
    }
}
