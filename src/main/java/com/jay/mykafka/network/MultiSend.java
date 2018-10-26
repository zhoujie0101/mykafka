package com.jay.mykafka.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.List;

/**
 * jie.zhou
 * 2018/10/25 17:25
 */
public class MultiSend implements Send {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiSend.class);

    private int expectedBytesToWrite;
    private List<? extends Send> sends;
    private Iterator<? extends Send> iter;
    private Send current;
    private int totalWritten = 0;

    public MultiSend(List<? extends Send> sends) {
        this.sends = sends;
        iter = sends.iterator();
        current = iter.next();
    }

    @Override
    public int writeTo(GatheringByteChannel channel) {
        expectIncomplete();
        int written = current.writeTo(channel);
        totalWritten += written;
        if (current.complete()) {
            current = iter.next();
        }

        return written;
    }

    @Override
    public boolean complete() {
        if (current == null) {
            if (totalWritten != expectedBytesToWrite) {
                LOGGER.error("mismatch in sending bytes over socket; expected: " + expectedBytesToWrite
                        + " actual: " + totalWritten);
            }
            return true;
        } else {
            return false;
        }
    }
}
