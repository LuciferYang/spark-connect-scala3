package org.apache.spark.sql;

/** Java factory for {@link Row}. */
public final class RowFactory {
  private RowFactory() {}

  public static Row create(Object... values) {
    return Row$.MODULE$.fromArray(values);
  }
}
