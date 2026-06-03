package org.apache.spark.sql;

final class JavaRelationalGroupedDatasetCompileTest {
  private RelationalGroupedDataset grouped;

  RelationalGroupedDataset identity(RelationalGroupedDataset value) {
    this.grouped = value;
    return this.grouped;
  }
}
