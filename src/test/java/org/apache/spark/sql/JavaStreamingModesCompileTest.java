package org.apache.spark.sql;

final class JavaStreamingModesCompileTest {
  org.apache.spark.sql.streaming.OutputMode outputMode() {
    org.apache.spark.sql.streaming.OutputMode append =
        org.apache.spark.sql.streaming.OutputMode.Append();
    org.apache.spark.sql.streaming.OutputMode update =
        org.apache.spark.sql.streaming.OutputMode.Update();
    return org.apache.spark.sql.streaming.OutputMode.Complete();
  }

  org.apache.spark.sql.streaming.TimeMode timeMode() {
    org.apache.spark.sql.streaming.TimeMode none =
        org.apache.spark.sql.streaming.TimeMode.None();
    org.apache.spark.sql.streaming.TimeMode processing =
        org.apache.spark.sql.streaming.TimeMode.ProcessingTime();
    return org.apache.spark.sql.streaming.TimeMode.EventTime();
  }

  org.apache.spark.sql.streaming.GroupStateTimeout groupStateTimeout() {
    org.apache.spark.sql.streaming.GroupStateTimeout none =
        org.apache.spark.sql.streaming.GroupStateTimeout.NoTimeout();
    org.apache.spark.sql.streaming.GroupStateTimeout processing =
        org.apache.spark.sql.streaming.GroupStateTimeout.ProcessingTimeTimeout();
    return org.apache.spark.sql.streaming.GroupStateTimeout.EventTimeTimeout();
  }
}
