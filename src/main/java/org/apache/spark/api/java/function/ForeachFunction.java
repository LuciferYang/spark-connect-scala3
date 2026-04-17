package org.apache.spark.api.java.function;

import java.io.Serializable;

/** Base interface for a function used in Dataset's foreach function. */
@FunctionalInterface
public interface ForeachFunction<T> extends Serializable {
  void call(T t) throws Exception;
}
