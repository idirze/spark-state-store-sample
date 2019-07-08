package com.idirze.bigdata.examples.streaming.continuous.utils;

import com.idirze.bigdata.examples.streaming.continuous.exception.StateStoreBackendInstantiationException;
import com.idirze.bigdata.examples.streaming.continuous.state.CustomStateStoreBackend;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.execution.streaming.state.StateStoreConf;
import org.apache.spark.sql.execution.streaming.state.StateStoreId;
import org.apache.spark.sql.execution.streaming.state.UnsafeRowPair;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.collection.Iterator;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StateStoreUtilsTest extends UnsafeRowUtilsMock {

    @Test
    @DisplayName("Create custom state store and ensure is working")
    public void create_custom_state_store_test() {

        StructType keySchema = new StructType()
                .add("k1", DataTypes.LongType);

        StructType valueSchema = new StructType()
                .add("v1", DataTypes.StringType);

        SQLConf sqlConf = new SQLConf();

        StateStoreConf storeConf = new StateStoreConf(sqlConf);

        Configuration hadoopConf = new Configuration();
        StateStoreId stateStoreId = new StateStoreId("",
                0l,
                1,
                "create_custom_state_store_test");

        CustomStateStoreBackend stateStore = StateStoreUtils
                .createStateStoreBackand("com.idirze.bigdata.examples.streaming.continuous.state.memory.MemoryStateStoreBackend"
                        , stateStoreId, keySchema, valueSchema);

        stateStore.put(makeKeyRow("key1"), makeValueRow("value1"));
        stateStore.put(makeKeyRow("key2"), makeValueRow("value2"));

        Map<String, String> result = new HashMap<>();

        Iterator<UnsafeRowPair> it = stateStore.iterator();
        while (it.hasNext()) {
            UnsafeRowPair next = it.next();
            result.put(next.key().getUTF8String(0).toString(), next.value().getUTF8String(0).toString());
        }

        assertThat(result).containsEntry("key1", "value1");
        assertThat(result).containsEntry("key2", "value2");

    }

    @Test()
    @DisplayName("Throw StateStoreInstantiationException if the state store impl class is not found")
    public void should_throw_StateStoreInstantiationException_when_state_store_class_not_found_test() {
        assertThrows(StateStoreBackendInstantiationException.class,
                () ->
                        StateStoreUtils
                                .createStateStoreBackand("com.idirze.bigdata.examples.streaming.continuous.state.memory.NoStateStore"
                                        , null, null, null, null, null)
        );
    }

}
