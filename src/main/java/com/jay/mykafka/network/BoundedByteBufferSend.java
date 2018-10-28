package com.jay.mykafka.network;

import com.jay.mykafka.api.Request;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/**
 * jie.zhou
 * 2018/10/28 10:23
 */
public class BoundedByteBufferSend implements Send {
    private ByteBuffer buffer;
    private ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
    private boolean complete = false;

    public BoundedByteBufferSend(int size) {
        this(ByteBuffer.allocate(size));
    }

    public BoundedByteBufferSend(Request req) {
        this(req.sizeInBytes() + 2);
        buffer.putShort(req.getId());
        req.writeTo(buffer);
        buffer.rewind();
    }

    public BoundedByteBufferSend(ByteBuffer buffer) {
        if (buffer.limit() > Integer.MAX_VALUE - sizeBuffer.limit()) {
            throw new IllegalArgumentException("Attempt to create a bounded buffer of " + buffer.remaining()
                    + " bytes, but the maximum " + "allowable size for a bounded buffer is "
                    + (Integer.MAX_VALUE  - sizeBuffer.limit()) + ".");
        }
        this.buffer = buffer;
        sizeBuffer.putInt(buffer.limit());
        sizeBuffer.rewind();
    }

    @Override
    public int writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        long written = channel.write(new ByteBuffer[]{sizeBuffer, buffer});
        if (!buffer.hasRemaining()) {
            complete = true;
        }

        return (int) written;
    }

    @Override
    public boolean complete() {
        return complete;
    }
}
