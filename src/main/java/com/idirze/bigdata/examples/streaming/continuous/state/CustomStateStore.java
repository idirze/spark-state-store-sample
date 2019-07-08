package com.idirze.bigdata.examples.streaming.continuous.state;

import com.idirze.bigdata.examples.streaming.continuous.constants.StoreState;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.streaming.state.*;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.immutable.HashMap;

import static com.google.common.base.Preconditions.checkState;
import static com.idirze.bigdata.examples.streaming.continuous.constants.StateStoreConstants.STATE_STORE_BACKEND;
import static com.idirze.bigdata.examples.streaming.continuous.constants.StoreState.Committed;
import static com.idirze.bigdata.examples.streaming.continuous.constants.StoreState.Updating;
import static com.idirze.bigdata.examples.streaming.continuous.constants.StateStoreBackendType.backendOf;
import static com.idirze.bigdata.examples.streaming.continuous.utils.StateStoreUtils.createStateStoreBackand;
import static com.idirze.bigdata.examples.streaming.continuous.utils.StateStoreUtils.getConfAsString;

public class CustomStateStore implements StateStore {

    private Long version;
    private StoreState state = Updating;
    private long newVersion;

    private StateStoreId stateStoreId;
    private StructType keySchema;
    private StructType valueSchema;
    private StateStoreConf storeConf;
    private Configuration hadoopConf;

    // Replace the store by Hbase store (Concurrency => rowkey : data key + operatorId + partitionId)
    private /* Shared or One cache instance by executor */ CustomStateStoreBackend store;

    public CustomStateStore(StateStoreId stateStoreId,
                            StructType keySchema,
                            StructType valueSchema,
                            StateStoreConf storeConf,
                            Configuration hadoopConf) {

        this.stateStoreId = stateStoreId;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.storeConf = storeConf;
        this.hadoopConf = hadoopConf;

        this.store = createStateStoreBackand(backendOf(getConfAsString(storeConf, STATE_STORE_BACKEND)).backendClass(),
                stateStoreId,
                keySchema,
                valueSchema);
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
        return store.get(key);
    }

    @Override
    public void put(UnsafeRow key, UnsafeRow value) {
        checkState(state == Updating, "Cannot put entry into an already committed or aborted state");
        store.put(key, value);
    }

    @Override
    public void remove(UnsafeRow key) {
        checkState(state == Updating, "Cannot remove entry from an already committed or aborted state");
        store.remove(key);
    }

    @Override
    public Iterator<UnsafeRowPair> getRange(Option<UnsafeRow> start, Option<UnsafeRow> end) {
        checkState(state == Updating, "Cannot get range from an already committed or aborted state");
        return store.range(start, end);
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
        return store.iterator();
    }

    @Override
    public StateStoreMetrics metrics() {
        return new StateStoreMetrics(store.size(),
                store.estimatedSize(),
                new HashMap<>());
    }

    @Override
    public boolean hasCommitted() {
        return state == Committed;
    }

    public CustomStateStore version(long version) {
        this.version = version;
        this.newVersion = version + 1;
        return this;
    }

    public CustomStateStore state(StoreState state) {
        this.state = state;
        return this;
    }

}
