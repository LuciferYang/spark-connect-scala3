package org.apache.spark.api.java.function;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.streaming.GroupState;

/** Base interface for a map function used in KeyValueGroupedDataset.mapGroupsWithState. */
@Evolving
@FunctionalInterface
public interface MapGroupsWithStateFunction<K, V, S, R> extends Serializable {
  R call(K key, Iterator<V> values, GroupState<S> state) throws Exception;
}
