package com.jay.mykafka.message;

import com.jay.mykafka.common.UnknownMagicByteException;
import com.jay.mykafka.util.Utils;

import java.nio.ByteBuffer;

/**
 * A message. The format of an N byte message is the following:
 *
 * If magic byte is 0
 *
 * 1. 1 byte "magic" identifier to allow format changes
 *
 * 2. 4 byte CRC32 of the payload
 *
 * 3. N - 5 byte payload
 *
 * If magic byte is 1
 *
 * 1. 1 byte "magic" identifier to allow format changes
 *
 * 2. 1 byte "attributes" identifier to allow annotations on the message independent of the version (e.g. compression enabled, type of codec used)
 *
 * 3. 4 byte CRC32 of the payload
 *
 * 4. N - 6 byte payload
 *
 * jie.zhou
 * 2018/10/26 17:23
 */
public class Message {

    public static final byte MAGIC_VERSION_1 = 0;
    public static final byte MAGIC_VERSION_2 = 1;
    public static final byte CURRENT_MAGIC_VALUE = 1;
    public static final byte MAGIC_OFFSET = 0;
    public static final byte MAGIC_LENGTH = 1;
    public static final byte ATTRIBUTE_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    public static final byte ATTRIBUTE_LENGTH = 1;
    public static final byte CRC_LENGTH = 4;

    private ByteBuffer buffer;

    public Message(byte[] bytes) {
        this(Utils.crc32(bytes), bytes);
    }

    public Message(long checksum, byte[] bytes) {
        buffer = ByteBuffer.allocate(headerSize(CURRENT_MAGIC_VALUE) + bytes.length);
        buffer.put(CURRENT_MAGIC_VALUE);
        byte attributes = 0;
        //TODO consider compress
        buffer.put(attributes);
        buffer.putInt((int) (checksum & 0xffffffffL));
        buffer.put(bytes);
        buffer.rewind();
    }

    public int size() {
        return buffer.limit();
    }

    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }

    public int payloadSize() {
        return size() - headerSize(magic());
    }

    public byte attributes() {
        return buffer.get(ATTRIBUTE_OFFSET);
    }

    public long checksum() {
        return buffer.get(crcOffset(magic()));
    }

    public ByteBuffer payload() {
        ByteBuffer payload = buffer.duplicate();
        payload.position(headerSize(magic()));
        payload = payload.slice();
        payload.limit(payloadSize());
        payload.rewind();

        return payload;
    }

    public boolean isValid() {
        return checksum() == Utils.crc32(buffer.array(),
                buffer.position() + buffer.arrayOffset() + payloadOffset(magic()), payloadSize());
    }

    public int serializedSize() {
        return 4  // length
                + buffer.limit();
    }

    public ByteBuffer serializedTo(ByteBuffer srcBuffer) {
        srcBuffer.putInt(buffer.limit());
        return srcBuffer.put(buffer.duplicate());
    }

    /**
     * Computes the CRC value based on the magic byte
     * @param magic Specifies the magic byte value. Possible values are 0 and 1
     *              0 for no compression
     *              1 for compression
     */
    public static int crcOffset(byte magic) {
        switch (magic) {
            case MAGIC_VERSION_1:
                return MAGIC_OFFSET + MAGIC_LENGTH;
            case MAGIC_VERSION_2:
                return ATTRIBUTE_OFFSET + ATTRIBUTE_LENGTH;
                default:
                    throw new UnknownMagicByteException("Magic byte value of " + magic + " is unknown");
        }
    }

    /**
     * Computes the offset to the message payload based on the magic byte
     * @param magic Specifies the magic byte value. Possible values are 0 and 1
     *              0 for no compression
     *              1 for compression
     */
    public static int payloadOffset(byte magic) {
        return crcOffset(magic) + CRC_LENGTH;
    }

    /**
     * Computes the size of the message header based on the magic byte
     * @param magic Specifies the magic byte value. Possible values are 0 and 1
     *              0 for no compression
     *              1 for compression
     */
    public static int headerSize(byte magic) {
        return payloadOffset(magic);
    }

    public static int minHeaderSize() {
        return headerSize((byte) 0);
    }
}
