package org.apache.spark.api.java.function;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.streaming.GroupState;

/** Base interface for a flatMap function used in KeyValueGroupedDataset.flatMapGroupsWithState. */
@Evolving
@FunctionalInterface
public interface FlatMapGroupsWithStateFunction<K, V, S, R> extends Serializable {
  Iterator<R> call(K key, Iterator<V> values, GroupState<S> state) throws Exception;
}
