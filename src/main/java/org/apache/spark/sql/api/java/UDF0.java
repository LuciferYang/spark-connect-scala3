package org.apache.spark.sql.api.java;

import java.io.Serializable;

/** A Spark SQL UDF that has 0 arguments. */
@FunctionalInterface
public interface UDF0<R> extends Serializable {
  R call() throws Exception;
}
