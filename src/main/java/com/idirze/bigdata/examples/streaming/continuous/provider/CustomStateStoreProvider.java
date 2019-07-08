package com.idirze.bigdata.examples.streaming.continuous.provider;

import com.google.common.collect.Lists;
import com.idirze.bigdata.examples.streaming.continuous.constants.StateStoreConstants;
import com.idirze.bigdata.examples.streaming.continuous.state.CustomStateStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.execution.streaming.state.*;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.Seq;

import static com.idirze.bigdata.examples.streaming.continuous.constants.StoreState.Updating;
import static com.idirze.bigdata.examples.streaming.continuous.utils.StateStoreUtils.getConfAsString;
import static scala.collection.JavaConversions.asScalaBuffer;

@Slf4j
public class CustomStateStoreProvider implements StateStoreProvider, StateStoreConstants {

    private StateStoreId stateStoreId;
    private StructType keySchema;
    private StructType valueSchema;
    private StateStoreConf storeConf;
    private Configuration hadoopConf;
    private CustomStateStore stateStore;

    @Override
    public void init(StateStoreId stateStoreId,
                     StructType keySchema,
                     StructType valueSchema, Option<Object> keyIndexOrdinal,
                     StateStoreConf storeConfs,
                     Configuration hadoopConf) {


        this.stateStoreId = stateStoreId;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.storeConf = storeConfs;
        this.hadoopConf = hadoopConf;

        this.stateStore = new CustomStateStore(stateStoreId,
                keySchema,
                valueSchema,
                storeConf,
                hadoopConf);

    }

    @Override
    public StateStoreId stateStoreId() {
        return stateStoreId;
    }

    @Override
    public void close() {
        // Close the state store
    }

    @Override
    public StateStore getStore(long version) {
        log.debug("Get version {} of the state store", version);
        // We return the latest version of the store, which ensures the at most guarantee
        // The resulting batch versions of the retries will be audited
        return stateStore
                .state(Updating)
                .version(version);
    }

    @Override
    public void doMaintenance() {
        // This method is invoked asynchrounsly by spark (one single thread per executor)
        // Since we are synchrounous, nothing todo
        // Could be used to purge the state store like hbase to limit the size of the table (if the keys are functional)
        // For Hbase, set the TTL to automatically expire the row keys (if the keys are technical)
        // Also, think of HBase coprocessor for functional purge of duplicates
    }

    @Override
    public Seq<StateStoreCustomMetric> supportedCustomMetrics() {
        return asScalaBuffer(Lists.<StateStoreCustomMetric>newArrayList()).toSeq();
    }
}
