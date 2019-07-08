package com.idirze.bigdata.examples.streaming.continuous.utils;

import com.idirze.bigdata.examples.streaming.continuous.exception.StateStoreBackendInstantiationException;
import com.idirze.bigdata.examples.streaming.continuous.state.CustomStateStoreBackend;
import org.apache.spark.sql.execution.streaming.state.StateStoreConf;
import org.apache.spark.sql.execution.streaming.state.StateStoreId;
import org.apache.spark.sql.types.StructType;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class StateStoreUtils {

    private static Class[] argsClass = new Class[]{
            StateStoreId.class,
            StructType.class,
            StructType.class};

    public static CustomStateStoreBackend createStateStoreBackand(String stateStoreClass, Object... args) {

        try {
            Class clazz = Class.forName(stateStoreClass);
            Constructor constructor = clazz.getConstructor(argsClass);
            return (CustomStateStoreBackend) constructor.newInstance(args);
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException | ClassNotFoundException | NoSuchMethodException e) {
            throw new StateStoreBackendInstantiationException("Failed to instantiate stateStore class: " + stateStoreClass, e);
        }
    }

    public static String getConfAsString(StateStoreConf storeConf, String option) {

        if (storeConf.confs().get(option).isDefined()) {
            return storeConf.confs().get(option).get();
        }

        return getConfAsString(storeConf, option, null);
    }

    public static String getConfAsString(StateStoreConf storeConf, String option, String defaultValue) {

        if (storeConf.confs().get(option).isDefined()) {
            return storeConf.confs().get(option).get();
        }

        return defaultValue;
    }

}
