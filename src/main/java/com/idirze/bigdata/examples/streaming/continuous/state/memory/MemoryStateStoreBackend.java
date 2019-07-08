package com.idirze.bigdata.examples.streaming.continuous.state.memory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.idirze.bigdata.examples.streaming.continuous.state.CustomStateStoreBackend;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.streaming.state.StateStoreId;
import org.apache.spark.sql.execution.streaming.state.UnsafeRowPair;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SizeEstimator;
import scala.Option;
import scala.collection.Iterator;

import java.nio.ByteBuffer;

import static com.idirze.bigdata.examples.streaming.continuous.utils.SerializerUtils.serialize;
import static java.nio.ByteBuffer.wrap;
import static java.util.concurrent.TimeUnit.MINUTES;

@Slf4j
public class MemoryStateStoreBackend implements CustomStateStoreBackend {

    private StateStoreId stateStoreId;
    private StructType keySchema;
    private StructType valueSchema;

    private /* Shared or One cache instance by executor */ Cache<ByteBuffer, byte[]> store;
    private MemoryStateStoreIterator iterator;

    public MemoryStateStoreBackend(StateStoreId stateStoreId,
                                   StructType keySchema,
                                   StructType valueSchema) {

        this.stateStoreId = stateStoreId;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;

        this.store = CacheBuilder
                .newBuilder()
                .expireAfterWrite(15, MINUTES)
                .build();

        this.iterator = new MemoryStateStoreIterator(keySchema, valueSchema, this.store);
    }

    @Override
    public UnsafeRow get(UnsafeRow key) {
        // To prevent concurrency issues, we suffix operatorId and partitionId for each key
        //spark.task.cpus => 1
        byte[] valueBytes = store.getIfPresent(wrap(serialize(key.getBytes(),
                stateStoreId.operatorId(),
                stateStoreId.partitionId())));

        if (valueBytes == null) {
            // Publish the data to the sink
            log.debug("DDE - Value not yet seen");
            return null;
        }

        // Send the duplicates into an audit topic
        // - In case of failure or retry the data will may not be pudblished to the sink
        // - Set the duplicate score:
        //   You can distinguish between normal duplicates & technical ones by versionning (current version < stored currentVersion  => retry)
        log.debug("DDE - Value already seen and maybe sent!");
        val value = new UnsafeRow(valueSchema.fields().length);
        value.pointTo(valueBytes, valueBytes.length);

        return value;
    }

    @Override
    public void put(UnsafeRow key, UnsafeRow value) {

        UnsafeRow keyCopy = key.copy();
        UnsafeRow valueCopy = value.copy();

        store.put(wrap(serialize(keyCopy.getBytes(),
                stateStoreId.operatorId(),
                stateStoreId.partitionId())),
                valueCopy.getBytes());
    }

    @Override
    public void remove(UnsafeRow key) {
        store.invalidate(key.getBytes());
    }

    @Override
    public Iterator<UnsafeRowPair> iterator() {
        return iterator.newIterator();
    }

    @Override
    public Iterator<UnsafeRowPair> range(Option<UnsafeRow> start, Option<UnsafeRow> end) {
        return iterator.newIterator();
    }

    @Override
    public long size() {
        return store.size();
    }

    @Override
    public long estimatedSize() {
        return SizeEstimator.estimate(store);
    }

}
