package com.idirze.bigdata.examples.streaming.continuous.state.memory;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.execution.streaming.state.StateStoreConf;
import org.apache.spark.sql.execution.streaming.state.StateStoreId;
import org.apache.spark.sql.types.StructType;


@Slf4j
public class MemoryStateStoreMock extends MemoryStateStore {


    public MemoryStateStoreMock(StateStoreId stateStoreId,
                                StructType keySchema,
                                StructType valueSchema,
                                StateStoreConf storeConf,
                                Configuration hadoopConf) {
        super(stateStoreId, keySchema, valueSchema, storeConf, hadoopConf);
    }

}
