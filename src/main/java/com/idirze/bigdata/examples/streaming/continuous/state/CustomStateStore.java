package com.idirze.bigdata.examples.streaming.continuous.state;

import com.idirze.bigdata.examples.streaming.continuous.constants.StoreState;
import com.idirze.bigdata.examples.streaming.continuous.state.memory.MemoryStateStore;
import org.apache.spark.sql.execution.streaming.state.StateStore;
import org.apache.spark.sql.execution.streaming.state.UnsafeRowPair;
import scala.collection.Iterator;

public interface CustomStateStore extends StateStore {

    MemoryStateStore version(long version);

    MemoryStateStore state(StoreState state);

    Iterator<UnsafeRowPair> newIterator();
}
