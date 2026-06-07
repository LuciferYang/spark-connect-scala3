package org.apache.spark.sql;

final class JavaEncodersCompatibilityCompileTest {
  Encoder<String> bean() {
    return Encoders.bean(String.class);
  }

  Encoder<String> kryo() {
    return Encoders.kryo(String.class);
  }

  Encoder<String> javaSerialization() {
    return Encoders.javaSerialization(String.class);
  }

  Encoder<org.apache.spark.sql.types.Geometry> geometry() {
    return Encoders.GEOMETRY(new org.apache.spark.sql.types.GeometryType(4326));
  }

  Encoder<org.apache.spark.sql.types.Geography> geography() {
    return Encoders.GEOGRAPHY(new org.apache.spark.sql.types.GeographyType(4326));
  }
}
