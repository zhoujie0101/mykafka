package com.jay.mykafka.common;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * jie.zhou
 * 2018/10/26 20:38
 */
public class ErrorMapping {
    public static ByteBuffer emptyByteBuffer = ByteBuffer.allocate(0);

    public static final int UNKNOWN_CODE = -1;
    public static final int NO_ERROR = 0;
    public static final int OFFSET_OUT_OF_RANGE_CODE = 1;
    public static final int INVALID_MESSAGE_CODE = 2;
    public static final int WRONG_PARTITION_CODE = 3;
    public static final int INVALID_FETCH_SIZE_CODE = 4;

    private static Map<Class<? extends Throwable>, Integer> exceptionToCode = new HashMap<>();
    private static Map<Integer, Class<? extends Throwable>> codeToException = new HashMap<>();
    static {
        exceptionToCode.put(UnknownException.class, UNKNOWN_CODE);
        codeToException.put(UNKNOWN_CODE, UnknownException.class);
        exceptionToCode.put(OffsetOutOfRangeException.class, OFFSET_OUT_OF_RANGE_CODE);
        codeToException.put(OFFSET_OUT_OF_RANGE_CODE, OffsetOutOfRangeException.class);
        exceptionToCode.put(InvalidMessageException.class, INVALID_MESSAGE_CODE);
        codeToException.put(INVALID_MESSAGE_CODE, InvalidMessageException.class);
        exceptionToCode.put(InvalidPartitionException.class, WRONG_PARTITION_CODE);
        codeToException.put(WRONG_PARTITION_CODE, InvalidPartitionException.class);
        exceptionToCode.put(InvalidMessageSizeException.class, INVALID_FETCH_SIZE_CODE);
        codeToException.put(INVALID_FETCH_SIZE_CODE, InvalidMessageSizeException.class);
    }

    public static void maybeThrowException(int code) throws Throwable {
        if (code != 0) {
            throw codeToException.get(code).newInstance();
        }
    }
}