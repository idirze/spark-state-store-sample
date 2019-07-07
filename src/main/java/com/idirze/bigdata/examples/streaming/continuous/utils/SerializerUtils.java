package com.idirze.bigdata.examples.streaming.continuous.utils;

import com.idirze.bigdata.examples.streaming.continuous.state.DedupeKey;
import org.apache.commons.lang3.SerializationUtils;

public class SerializerUtils {

    public static byte[] serialize(byte[] unsafeRow, long operatorId, int partitionId) {
        return SerializationUtils.serialize(DedupeKey
                .builder()
                .unsafeRowBytes(unsafeRow)
                .partitionId(partitionId)
                .operatorId(operatorId)
                .build());
    }

    public static DedupeKey deserialize(byte[] unsafeRow) {
        return SerializationUtils.deserialize(unsafeRow);
    }


}
