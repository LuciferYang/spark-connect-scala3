package org.apache.spark.api.java.function;

import java.io.Serializable;

/** Base interface for a function used in Dataset's filter function. */
@FunctionalInterface
public interface FilterFunction<T> extends Serializable {
  boolean call(T value) throws Exception;
}
