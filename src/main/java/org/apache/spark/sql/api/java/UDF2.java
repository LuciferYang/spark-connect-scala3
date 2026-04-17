package org.apache.spark.sql.api.java;

import java.io.Serializable;

/** A Spark SQL UDF that has 2 arguments. */
@FunctionalInterface
public interface UDF2<T1, T2, R> extends Serializable {
  R call(T1 t1, T2 t2) throws Exception;
}
