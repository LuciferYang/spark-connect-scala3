package org.apache.spark.sql.api.java;

import java.io.Serializable;

/** A Spark SQL UDF that has 5 arguments. */
@FunctionalInterface
public interface UDF5<T1, T2, T3, T4, T5, R> extends Serializable {
  R call(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) throws Exception;
}
