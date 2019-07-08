package com.idirze.bigdata.examples.streaming.continuous.state.hbase;


import com.idirze.bigdata.examples.streaming.continuous.state.CustomStateStoreBackend;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.streaming.state.StateStoreId;
import org.apache.spark.sql.execution.streaming.state.UnsafeRowPair;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.Iterator;

public class HBaseStateStoreBackend implements CustomStateStoreBackend {

    private StateStoreId stateStoreId;
    private StructType keySchema;
    private StructType valueSchema;

    private HBaseStateStoreIterator iterator;

    public HBaseStateStoreBackend(StateStoreId stateStoreId,
                                   StructType keySchema,
                                   StructType valueSchema) {

        this.stateStoreId = stateStoreId;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;

        this.iterator = new HBaseStateStoreIterator(keySchema, valueSchema, null);
    }

    @Override
    public UnsafeRow get(UnsafeRow key) {
        return null;
    }

    @Override
    public void put(UnsafeRow key, UnsafeRow value) {

    }

    @Override
    public void remove(UnsafeRow key) {

    }

    @Override
    public Iterator<UnsafeRowPair> iterator() {
        return null;
    }

    @Override
    public Iterator<UnsafeRowPair> range(Option<UnsafeRow> start, Option<UnsafeRow> end) {
        return null;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public long estimatedSize() {
        return 0;
    }
}
