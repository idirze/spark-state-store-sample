package com.idirze.bigdata.examples.streaming.continuous.constants;

import com.idirze.bigdata.examples.streaming.continuous.utils.StateStoreUtils;
import org.apache.spark.sql.execution.streaming.state.StateStoreConf;

public interface StateStoreConstants {

    String STATE_STORE_CLASS = "spark.sql.streaming.stateStore.stateStoreClass";

    // Add other parameters here like: kafka broker list, audit topic, windowSizeMinutes, etc
    // The parameters should begin with spark.sql.streaming.stateStore. to be picked by spark
    /**
     * Example Usage: {@link StateStoreUtils#getConfAsString(StateStoreConf, String)}  }
     */
}
