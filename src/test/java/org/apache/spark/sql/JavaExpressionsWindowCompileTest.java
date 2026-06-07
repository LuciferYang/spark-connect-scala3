package org.apache.spark.sql;

final class JavaExpressionsWindowCompileTest {
  Column useExpressionsWindowPackage() {
    org.apache.spark.sql.expressions.WindowSpec spec =
        org.apache.spark.sql.expressions.Window.partitionBy("dept")
            .orderBy(org.apache.spark.sql.functions.col("salary"))
            .rowsBetween(
                org.apache.spark.sql.expressions.Window.unboundedPreceding(),
                org.apache.spark.sql.expressions.Window.currentRow());

    return org.apache.spark.sql.functions.row_number().over(spec);
  }
}
