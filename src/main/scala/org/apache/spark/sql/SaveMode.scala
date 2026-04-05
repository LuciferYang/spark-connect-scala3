package org.apache.spark.sql

/** Save modes for writing DataFrames to external storage. */
enum SaveMode:
  case Overwrite, Append, Ignore, ErrorIfExists
