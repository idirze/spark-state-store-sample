package com.idirze.bigdata.examples.streaming.continuous.constants;

import com.idirze.bigdata.examples.streaming.continuous.utils.StateStoreUtils;
import org.apache.spark.sql.execution.streaming.state.StateStoreConf;

public interface StateStoreConstants {

    /**
     * The state store backend, possible values: memory (testing), hbase
     */
    String STATE_STORE_BACKEND = "spark.sql.streaming.stateStore.stateStoreBackend";

    // Add other parameters here like: hbase regions, table, kafka broker list, audit topic, windowSizeMinutes, etc
    // The parameters should begin with spark.sql.streaming.stateStore. to be picked by spark
    /**
     * Example Usage: {@link StateStoreUtils#getConfAsString(StateStoreConf, String)}  }
     */
}
