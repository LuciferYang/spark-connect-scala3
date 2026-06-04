package org.apache.spark.sql;

import java.util.Map;

final class JavaAvroProtobufFunctionsCompileTest {
  Column avro(Column bytes, Column value) {
    Column decoded =
        org.apache.spark.sql.avro.functions.from_avro(
            bytes, "{\"type\":\"int\",\"name\":\"id\"}", Map.of("mode", "FAILFAST"));
    Column encoded =
        org.apache.spark.sql.avro.functions.to_avro(
            value, "{\"type\":\"int\",\"name\":\"id\"}");
    return org.apache.spark.sql.avro.functions.schema_of_avro("{\"type\":\"int\",\"name\":\"id\"}");
  }

  Column protobuf(Column bytes, Column value) {
    byte[] descriptor = new byte[] {1, 2, 3};
    Column decoded =
        org.apache.spark.sql.protobuf.functions.from_protobuf(
            bytes, "StorageLevel", descriptor, Map.of("recursive.fields.max.depth", "2"));
    Column encoded =
        org.apache.spark.sql.protobuf.functions.to_protobuf(
            value, "StorageLevel", descriptor, Map.of("recursive.fields.max.depth", "2"));
    return encoded;
  }
}
