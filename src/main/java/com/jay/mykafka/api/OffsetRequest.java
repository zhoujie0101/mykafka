package com.jay.mykafka.api;

import com.jay.mykafka.common.ErrorMapping;
import com.jay.mykafka.network.Send;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.charset.StandardCharsets;

/**
 * jie.zhou
 * 2018/11/5 17:31
 */
public class OffsetRequest extends Request {

    public static final String SMALLEST_TIME = "smallest";

    public static final String LARGEST_TIME = "largest";

    public static final long LATEST_TIME = -1L;

    public static final long EARLIEST_TIME = -2L;

    private String topic;
    private int partition;
    private long time;
    private int maxNumOffset;

    public OffsetRequest(String topic, int partition, long time, int maxNumOffset) {
        super(RequestKeys.FETCH);
        this.topic = topic;
        this.partition = partition;
        this.time = time;
        this.maxNumOffset = maxNumOffset;
    }

    @Override
    public int sizeInBytes() {
        return 2  //topic length
                + topic.length()  //topic
                + 4 // partition
                + 8 // time
                + 4; // offset
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes(StandardCharsets.UTF_8));
        buffer.putInt(partition);
        buffer.putLong(time);
        buffer.putInt(maxNumOffset);
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getTime() {
        return time;
    }

    public int getMaxNumOffset() {
        return maxNumOffset;
    }

    public static OffsetRequest readFrom(ByteBuffer buffer) {
        short topicLength = buffer.getShort();
        byte[] bytes = new byte[topicLength];
        buffer.get(bytes);
        String topic = null;
        try {
            topic = new String(bytes, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        int partition = buffer.getInt();
        long time = buffer.getLong();
        int maxNumOffsets = buffer.getInt();

        return new OffsetRequest(topic, partition, time, maxNumOffsets);
    }

    private static ByteBuffer serializeOffsetArray(long[] offsets) {
        int size = 4 + offsets.length * 8;
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(offsets.length);
        for (int i = 0; i < offsets.length; i++) {
            buf.putLong(offsets[i]);
        }
        buf.rewind();

        return buf;
    }

    public static class OffsetArraySend implements Send {
        private long[] offsets;
        private long size;
        private ByteBuffer header;
        private ByteBuffer content;
        private boolean complete;

        public OffsetArraySend(long[] offsets) {
            this.offsets = offsets;
            size = 4 // length
                    + offsets.length * 8;
            header = ByteBuffer.allocate(6);
            header.putInt((int)size + 2);
            header.putShort((short) ErrorMapping.NO_ERROR);
            header.rewind();

            content = OffsetRequest.serializeOffsetArray(offsets);
        }

        @Override
        public int writeTo(GatheringByteChannel channel) throws IOException {
            expectIncomplete();
            int written = 0;
            if (header.hasRemaining()) {
                written += channel.write(header);
            }
            if (!header.hasRemaining() && content.hasRemaining()) {
                written += channel.write(content);
            }
            if (!content.hasRemaining()) {
                complete = true;
            }

            return written;
        }

        @Override
        public boolean complete() {
            return complete;
        }
    }
}
