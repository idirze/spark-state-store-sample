package com.idirze.bigdata.examples.streaming.continuous.state;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Builder
@Data
public class DedupeKey implements Serializable {

    private byte[] unsafeRowBytes;
    private long operatorId;
    private int partitionId;
}
