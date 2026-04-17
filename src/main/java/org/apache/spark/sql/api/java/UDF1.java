package org.apache.spark.sql.api.java;

import java.io.Serializable;

/** A Spark SQL UDF that has 1 argument. */
@FunctionalInterface
public interface UDF1<T1, R> extends Serializable {
  R call(T1 t1) throws Exception;
}
