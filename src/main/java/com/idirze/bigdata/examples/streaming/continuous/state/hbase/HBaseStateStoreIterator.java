package com.idirze.bigdata.examples.streaming.continuous.state.hbase;

import com.google.common.cache.Cache;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.streaming.state.UnsafeRowPair;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.idirze.bigdata.examples.streaming.continuous.utils.SerializerUtils.deserialize;
import static scala.collection.JavaConversions.asScalaIterator;

public class HBaseStateStoreIterator {

    private StructType keySchema;
    private StructType valueSchema;
    private Cache<ByteBuffer, byte[]> store;

    public HBaseStateStoreIterator(StructType keySchema,
                                    StructType valueSchema,
                                    Cache<ByteBuffer, byte[]> store) {

        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.store = store;

    }

    public Iterator<UnsafeRowPair> newIterator() {
        return asScalaIterator(new java.util.Iterator<UnsafeRowPair>() {

            private java.util.Iterator<Map.Entry<ByteBuffer, byte[]>> it = store.asMap().entrySet().iterator();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public UnsafeRowPair next() {

                Map.Entry<ByteBuffer, byte[]> pair = it.next();
                UnsafeRow key = new UnsafeRow(keySchema.fields().length);
                ByteBuffer byteBuffer = pair.getKey();

                byte[] keyBytes = new byte[byteBuffer.capacity()];
                byteBuffer.get(keyBytes, 0, keyBytes.length);

                byte[] dedupeKeyBytes = deserialize(keyBytes).getUnsafeRowBytes();

                key.pointTo(dedupeKeyBytes, dedupeKeyBytes.length);

                UnsafeRow value = new UnsafeRow(valueSchema.fields().length);
                byte[] valueBytes = pair.getValue();
                value.pointTo(valueBytes, valueBytes.length);

                return new UnsafeRowPair(key, value);

            }
        });
    }
}
