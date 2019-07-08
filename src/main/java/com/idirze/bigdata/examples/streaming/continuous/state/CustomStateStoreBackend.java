package com.idirze.bigdata.examples.streaming.continuous.state;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.streaming.state.UnsafeRowPair;
import scala.Option;
import scala.collection.Iterator;

public interface CustomStateStoreBackend {

    /**
     * Perform get operations on a single row.
     * @param key
     * @return
     */
    UnsafeRow get(UnsafeRow key);

    /**
     * perform put operations for a single row.
     * @param key
     * @param value
     */
    void put(UnsafeRow key, UnsafeRow value);

    /**
     * Delete an entire row
     * @param key
     */
    void remove(UnsafeRow key);

    /**
     * Iterate over the store
     * @return
     */
    Iterator<UnsafeRowPair> iterator();

    /**
     * Iterate over a range keys
     * @param start
     * @param end
     * @return
     */
    Iterator<UnsafeRowPair> range(Option<UnsafeRow> start, Option<UnsafeRow> end);

    /**
     * Size of the store, i.e.: Number of elements
     * @return
     */
    long size();

    /**
     * Estimated size of the cache (bytes)
     * @return
     */
    long estimatedSize();

}
