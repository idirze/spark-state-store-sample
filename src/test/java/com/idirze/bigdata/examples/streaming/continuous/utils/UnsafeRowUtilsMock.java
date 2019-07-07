package com.idirze.bigdata.examples.streaming.continuous.utils;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.unsafe.types.UTF8String;

public class UnsafeRowUtilsMock {

    public UnsafeRow makeKeyRow(String k1) {
        UnsafeRow row = new UnsafeRow(2);
        BufferHolder holder = new BufferHolder(row, 32);
        UnsafeRowWriter writer = new UnsafeRowWriter(holder, 2);
        holder.reset();
        writer.write(0, UTF8String.fromString(k1));
        row.setTotalSize(holder.totalSize());
        return row;
    }

    public UnsafeRow makeValueRow(String v1) {
        UnsafeRow row = new UnsafeRow(2);
        BufferHolder holder = new BufferHolder(row, 0);
        UnsafeRowWriter writer = new UnsafeRowWriter(holder, 2);
        holder.reset();
        writer.write(0, UTF8String.fromString(v1));
        row.setTotalSize(holder.totalSize());
        return row;
    }
}
