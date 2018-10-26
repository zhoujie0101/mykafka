package com.jay.mykafka.network;

/**
 * jie.zhou
 * 2018/10/25 17:20
 */
public interface Transmission {
    boolean complete();

    default void expectIncomplete() {
        if (complete()) {
            throw new IllegalStateException("This operation cannot be completed on a complete request.");
        }
    }

    default void expectComplete() {
        if (!complete()) {
            throw new IllegalStateException("This operation cannot be completed on a incomplete request.");
        }
    }
}
