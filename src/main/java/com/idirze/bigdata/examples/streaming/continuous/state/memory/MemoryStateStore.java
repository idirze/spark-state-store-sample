package com.idirze.bigdata.examples.streaming.continuous.state.memory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.idirze.bigdata.examples.streaming.continuous.constants.StoreState;
import com.idirze.bigdata.examples.streaming.continuous.state.CustomStateStore;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.streaming.state.StateStoreConf;
import org.apache.spark.sql.execution.streaming.state.StateStoreId;
import org.apache.spark.sql.execution.streaming.state.StateStoreMetrics;
import org.apache.spark.sql.execution.streaming.state.UnsafeRowPair;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SizeEstimator;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.immutable.HashMap;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static com.idirze.bigdata.examples.streaming.continuous.constants.StoreState.Committed;
import static com.idirze.bigdata.examples.streaming.continuous.constants.StoreState.Updating;
import static com.idirze.bigdata.examples.streaming.continuous.utils.SerializerUtils.serialize;
import static java.nio.ByteBuffer.wrap;

@Slf4j
public class MemoryStateStore implements CustomStateStore {

    private Long version;
    private StoreState state = Updating;
    private long newVersion;

    private StateStoreId stateStoreId;
    private StructType keySchema;
    private StructType valueSchema;
    private StateStoreConf storeConf;
    private Configuration hadoopConf;

    // Replace the store by Hbase store (Concurrency => rowkey : data key + operatorId + partitionId)
    private static /* Shared or One cache instance by executor */ Cache<ByteBuffer, byte[]> store;
    private MemoryStateStoreIterator iterator;

    public MemoryStateStore(StateStoreId stateStoreId,
                            StructType keySchema,
                            StructType valueSchema,
                            StateStoreConf storeConf,
                            Configuration hadoopConf) {

        this.stateStoreId = stateStoreId;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.storeConf = storeConf;
        this.hadoopConf = hadoopConf;

        this.store = CacheBuilder
                .newBuilder()
                .expireAfterWrite(15, TimeUnit.MINUTES)
                .build();

        this.iterator = new MemoryStateStoreIterator(keySchema, valueSchema, this.store);
    }

    @Override
    public StateStoreId id() {
        return this.stateStoreId;
    }

    @Override
    public long version() {
        return this.version;
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
        checkState(state == Updating, "Cannot put entry into an already committed or aborted state");
        UnsafeRow keyCopy = key.copy();
        UnsafeRow valueCopy = value.copy();

        store.put(wrap(serialize(keyCopy.getBytes(),
                stateStoreId.operatorId(),
                stateStoreId.partitionId())),
                valueCopy.getBytes());
    }

    @Override
    public void remove(UnsafeRow key) {
        checkState(state == Updating, "Cannot remove entry from an already committed or aborted state");
        store.invalidate(key.getBytes());
    }

    @Override
    public Iterator<UnsafeRowPair> getRange(Option<UnsafeRow> start, Option<UnsafeRow> end) {

        checkState(state == Updating, "Cannot get range from an already committed or aborted state");
        return newIterator();
    }

    @Override
    public long commit() {
        checkState(state == Updating, "Cannot commit already committed or aborted state");
        // state = Committed;
        return newVersion;
    }

    @Override
    public void abort() {
        // checkState(state != Committed, "Cannot abort already committed state");
        // state = Aborted;
    }

    @Override
    public Iterator<UnsafeRowPair> iterator() {
        return newIterator();
    }

    @Override
    public StateStoreMetrics metrics() {
        return new StateStoreMetrics(store.size(),
                SizeEstimator.estimate(store),
                new HashMap<>());
    }

    @Override
    public boolean hasCommitted() {
        return state == Committed;
    }

    @Override
    public MemoryStateStore version(long version) {
        this.version = version;
        this.newVersion = version + 1;
        return this;
    }

    @Override
    public MemoryStateStore state(StoreState state) {
        this.state = state;
        return this;
    }

    @Override
    public Iterator<UnsafeRowPair> newIterator() {
        return iterator.newIterator();
    }

}
